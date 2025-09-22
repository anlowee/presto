/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.clp.split;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.optimization.ClpTopNSpec;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.ARCHIVE;
import static java.lang.String.format;

public class ClpMySqlSplitProvider
        implements ClpSplitProvider
{
    // Column names
    public static final String ARCHIVES_TABLE_COLUMN_ID = "id";
    public static final String ARCHIVES_TABLE_NUM_MESSAGES = "num_messages";

    // Table suffixes
    public static final String ARCHIVES_TABLE_SUFFIX = "_archives";

    // SQL templates
    private static final String SQL_SELECT_ARCHIVES_TEMPLATE = format("SELECT * FROM `%%s%%s%s` WHERE 1 = 1", ARCHIVES_TABLE_SUFFIX);

    private static final Logger log = Logger.get(ClpMySqlSplitProvider.class);

    private final ClpConfig config;

    @Inject
    public ClpMySqlSplitProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
        this.config = config;
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        Optional<ClpTopNSpec> topNSpecOptional = clpTableLayoutHandle.getTopN();
        String tablePath = clpTableHandle.getTablePath();
        String tableName = clpTableHandle.getSchemaTableName().getTableName();
        String archivePathQuery = format(SQL_SELECT_ARCHIVES_TEMPLATE, config.getMetadataTablePrefix(), tableName);

        if (clpTableLayoutHandle.getMetadataSql().isPresent()) {
            String metadataFilterQuery = clpTableLayoutHandle.getMetadataSql().get();
            archivePathQuery += " AND (" + metadataFilterQuery + ")";
        }

        if (topNSpecOptional.isPresent()) {
            ClpTopNSpec topNSpec = topNSpecOptional.get();
            // Only handles one range metadata column for now
            ClpTopNSpec.Ordering ordering = topNSpec.orderings.get(0);
            String col = ordering.columns.get(ordering.columns.size() - 1);
            String dir = (ordering.order == ClpTopNSpec.Order.ASC) ? "ASC" : "DESC";
            archivePathQuery += " ORDER BY " + "`" + col + "` " + dir;

            List<ArchiveMeta> archiveMetaList = fetchArchiveMeta(archivePathQuery, ordering);
            List<ArchiveMeta> selected = selectTopNArchives(archiveMetaList, topNSpec.limit, ordering.order);

            for (ArchiveMeta a : selected) {
                splits.add(new ClpSplit(tablePath + "/" + a.id, ARCHIVE, clpTableLayoutHandle.getKqlQuery()));
            }
            ImmutableList<ClpSplit> result = splits.build();
            log.debug("Number of splits: %s", result.size());
            return result;
        }
        log.debug("Query for archive: %s", archivePathQuery);

        try (Connection connection = getConnection()) {
            // Fetch archive IDs and create splits
            try (PreparedStatement statement = connection.prepareStatement(archivePathQuery); ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String archiveId = resultSet.getString(ARCHIVES_TABLE_COLUMN_ID);
                    final String archivePath = tablePath + "/" + archiveId;
                    splits.add(new ClpSplit(archivePath, ARCHIVE, clpTableLayoutHandle.getKqlQuery()));
                }
            }
        }
        catch (SQLException e) {
            log.warn("Database error while processing splits for %s: %s", tableName, e);
        }

        ImmutableList<ClpSplit> filteredSplits = splits.build();
        log.debug("Number of splits: %s", filteredSplits.size());
        return filteredSplits;
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
        String dbName = config.getMetadataDbName();
        if (dbName != null && !dbName.isEmpty()) {
            connection.createStatement().execute(format("USE `%s`", dbName));
        }
        return connection;
    }

    /**
     * Fetches archive metadata from the database.
     *
     * @param query    SQL query string that selects the archives
     * @param ordering The top-N ordering specifying which columns contain lowerBound/upperBound
     * @return List of ArchiveMeta objects representing archive metadata
     */
    private List<ArchiveMeta> fetchArchiveMeta(String query, ClpTopNSpec.Ordering ordering) {
        List<ArchiveMeta> list = new ArrayList<>();
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection.prepareStatement(query);
                ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                list.add(new ArchiveMeta(
                        rs.getString(ARCHIVES_TABLE_COLUMN_ID),
                        rs.getLong(ordering.columns.get(0)),
                        rs.getLong(ordering.columns.get(1)),
                        rs.getLong(ARCHIVES_TABLE_NUM_MESSAGES)
                ));
            }
        }
        catch (SQLException e) {
            log.warn("Database error while fetching archive metadata: %s", e);
        }
        return list;
    }

    /**
     * Selects archives to satisfy top-N messages in a safe way, considering overlapping timestamp
     * ranges.
     *
     * @param archives List of ArchiveMeta objects, already filtered by SQL
     * @param limit    Number of messages to fetch (top-N)
     * @param order    Ordering direction (ASC or DESC) for top-N selection
     * @return List of {@link ArchiveMeta} objects that must be scanned to safely fetch top-N
     * messages
     */
    private List<ArchiveMeta> selectTopNArchives(List<ArchiveMeta> archives, long limit, ClpTopNSpec.Order order) {
        List<ArchiveMeta> selected = new ArrayList<>();
        long accumulated = 0;

        if (order == ClpTopNSpec.Order.ASC) {
            archives.sort(Comparator.comparingLong(a -> a.lowerBound));
            for (ArchiveMeta a : archives) {
                if (accumulated < limit) {
                    selected.add(a);
                    accumulated += a.messageCount;
                } else {
                    long maxUpper = selected.stream().mapToLong(m -> m.upperBound).max().orElse(Long.MIN_VALUE);
                    if (a.lowerBound <= maxUpper) {
                        selected.add(a);
                        accumulated += a.messageCount;
                    } else {
                        break;
                    }
                }
            }
        }
        else { // DESC
            for (ArchiveMeta a : archives) {
                if (accumulated < limit) {
                    selected.add(a);
                    accumulated += a.messageCount;
                } else {
                    long minLower = selected.stream().mapToLong(m -> m.lowerBound).min().orElse(Long.MAX_VALUE);
                    if (a.upperBound >= minLower) {
                        selected.add(a);
                        accumulated += a.messageCount;
                    } else {
                        break;
                    }
                }
            }
        }
        return selected;
    }

    /**
     * Represents metadata of an archive, including its ID, timestamp bounds, and message count.
     */
    private static class ArchiveMeta
    {
        final String id;
        final long lowerBound;
        final long upperBound;
        final long messageCount;

        ArchiveMeta(String id, long lowerBound, long upperBound, long messageCount)
        {
            this.id = id;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.messageCount = messageCount;
        }
    }
}
