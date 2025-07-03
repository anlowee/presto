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
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClpMySqlSplitProvider
        implements ClpSplitProvider
{
    private static final Logger log = Logger.get(ClpMySqlSplitProvider.class);
    private final ClpConfig config;

    // Column names
    private static final String ARCHIVES_TABLE_COLUMN_ID = "id";
    private static final String DATASETS_TABLE_COLUMN_NAME = "name";
    private static final String DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY = "archive_storage_directory";

    // Table suffixes
    private static final String ARCHIVE_TABLE_SUFFIX = "_archives";
    private static final String DATASETS_TABLE_SUFFIX = "datasets";

    // SQL templates
    private static final String SQL_SELECT_ARCHIVES_TEMPLATE =
            String.format("SELECT %s FROM %%s%%s%s WHERE 1 = 1", ARCHIVES_TABLE_COLUMN_ID, ARCHIVE_TABLE_SUFFIX);
    private static final String SQL_SELECT_DATASETS_TEMPLATE =
            String.format("SELECT %s FROM %%s%s WHERE %s = ?", DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY,
                    DATASETS_TABLE_SUFFIX, DATASETS_TABLE_COLUMN_NAME);

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

    private Connection getConnection() throws SQLException
    {
        Connection connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
        String dbName = config.getMetadataDbName();
        if (dbName != null && !dbName.isEmpty()) {
            connection.createStatement().execute("USE `" + dbName + "`");
        }
        return connection;
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        List<ClpSplit> splits = new ArrayList<>();
        SchemaTableName tableSchemaName = clpTableLayoutHandle.getTable().getSchemaTableName();
        String tableName = tableSchemaName.getTableName();

        String tablePathQuery = String.format(SQL_SELECT_DATASETS_TEMPLATE, config.getMetadataTablePrefix(), tableName);
        String archivePathQuery = String.format(SQL_SELECT_ARCHIVES_TEMPLATE, config.getMetadataTablePrefix(), tableName);
        if (clpTableLayoutHandle.getMetadataFilterQuery().isPresent()) {
            String metadataFilterQuery = clpTableLayoutHandle.getMetadataFilterQuery().get();
            archivePathQuery += " AND (" + metadataFilterQuery + ")";
        }
        log.info("Query for table: %s", tablePathQuery);
        log.info("Query for archive: %s", archivePathQuery);

        try (Connection connection = getConnection()) {
            // Fetch table path
            String tablePath;
            try (PreparedStatement statement = connection.prepareStatement(tablePathQuery)) {
                statement.setString(1, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        log.warn("Table metadata not found for table: %s", tableName);
                        return ImmutableList.of();
                    }
                    tablePath = resultSet.getString(DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY);
                }
            }

            if (tablePath == null || tablePath.isEmpty()) {
                log.warn("Table path is null for table: %s", tableName);
                return ImmutableList.of();
            }

            // Fetch archive IDs and create splits
            try (PreparedStatement statement = connection.prepareStatement(archivePathQuery);
                    ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String archiveId = resultSet.getString(ARCHIVES_TABLE_COLUMN_ID);
                    final String archivePath = tablePath + "/" + archiveId;
                    splits.add(new ClpSplit(archivePath));
                }
            }
        }
        catch (SQLException e) {
            log.warn("Database error while processing splits for %s: %s", tableName, e);
        }

        String oldKql = clpTableLayoutHandle.getKqlQuery().orElse("id > 1");
        clpTableLayoutHandle.setKqlQuery(oldKql + " AND id < 1000000");
        log.info("Number of splits: %s", splits.size());
        return splits;
    }
}
