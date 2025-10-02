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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpSplit.SplitType;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.ARCHIVE;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.IR;
import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpPinotSplitProvider
        implements ClpSplitProvider
{
    private static final Logger log = Logger.get(ClpPinotSplitProvider.class);
    private static final String SQL_SELECT_SPLITS_TEMPLATE = "SELECT tpath FROM %s WHERE 1 = 1 AND (%s) LIMIT 999999";
    private static final String SQL_SELECT_SPLIT_META_TEMPLATE = "SELECT tpath, creationtime, lastmodifiedtime, num_messages FROM %s WHERE 1 = 1 AND (%s) ORDER BY %s %s LIMIT 999999";
    private final ClpConfig config;
    private URL pinotDatabaseUrl;

    @Inject
    public ClpPinotSplitProvider(ClpConfig config)
    {
        this.config = config;
        try {
            this.pinotDatabaseUrl = new URL(config.getMetadataDbUrl() + "/query/sql");
        }
        catch (MalformedURLException e) {
            log.error(e);
            this.pinotDatabaseUrl = null;
        }
        requireNonNull(pinotDatabaseUrl, "pinotDatabaseUrl is null");
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        Optional<ClpTopNSpec> topNSpecOptional = clpTableLayoutHandle.getTopN();
        String tableName = clpTableHandle.getSchemaTableName().getTableName();
        try {
            ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
            if (topNSpecOptional.isPresent()) {
                ClpTopNSpec topNSpec = topNSpecOptional.get();
                // Only handles one range metadata column for now
                ClpTopNSpec.Ordering ordering = topNSpec.orderings.get(0);
                String col = ordering.columns.get(ordering.columns.size() - 1);
                String dir = (ordering.order == ClpTopNSpec.Order.ASC) ? "ASC" : "DESC";
                String splitMetaQuery = format(SQL_SELECT_SPLIT_META_TEMPLATE, tableName, clpTableLayoutHandle.getMetadataSql().orElse("1 = 1"), col, dir);
                List<ArchiveMeta> archiveMetaList = fetchArchiveMeta(splitMetaQuery, ordering);
                List<ArchiveMeta> selected = selectTopNArchives(archiveMetaList, topNSpec.limit, ordering.order);

                for (ArchiveMeta a : selected) {
                    String splitPath = a.id;
                    SplitType splitType = splitPath.endsWith(".clp.zst") ? IR : ARCHIVE;
                    splits.add(new ClpSplit(splitPath, splitType, clpTableLayoutHandle.getKqlQuery()));
                }

                List<ClpSplit> filteredSplits = splits.build();
                log.debug("Number of topN filtered splits: %s", filteredSplits.size());
                return filteredSplits;
            }

            String splitQuery = format(SQL_SELECT_SPLITS_TEMPLATE, tableName, clpTableLayoutHandle.getMetadataSql().orElse("1 = 1"));
            List<JsonNode> splitRows = getQueryResult(pinotDatabaseUrl, splitQuery);
            for (JsonNode row : splitRows) {
                String splitPath = row.elements().next().asText();
                SplitType splitType = splitPath.endsWith(".clp.zst") ? IR : ARCHIVE;
                splits.add(new ClpSplit(splitPath, splitType, clpTableLayoutHandle.getKqlQuery()));
            }

            List<ClpSplit> filteredSplits = splits.build();
            log.debug("Number of filtered splits: %s", filteredSplits.size());
            return filteredSplits;
        }
        catch (Exception e) {
            log.error(e);
        }

        return Collections.emptyList();
    }

    /**
     * Fetches archive metadata from the database.
     *
     * @param query    SQL query string that selects the archives
     * @param ordering The top-N ordering specifying which columns contain lowerBound/upperBound
     * @return List of ArchiveMeta objects representing archive metadata
     */
    private List<ArchiveMeta> fetchArchiveMeta(String query, ClpTopNSpec.Ordering ordering)
    {
        ImmutableList.Builder<ArchiveMeta> archiveMetas = new ImmutableList.Builder<>();
        List<JsonNode> rows = getQueryResult(pinotDatabaseUrl, query);
        for (JsonNode row : rows) {
            archiveMetas.add(new ArchiveMeta(
                    row.get(0).asText(),
                    row.get(1).asLong(),
                    row.get(2).asLong(),
                    row.get(3).asLong()));
        }
        return archiveMetas.build();
    }

    /**
     * Selects the set of archives that must be scanned to guarantee the top-N results by timestamp
     * (ASC or DESC), given only archive ranges and message counts.
     * <ul>
     *   <li>Merges overlapping archives into components (union of time ranges).</li>
     *   <li>For DESC: always include the newest component, then add older ones until their total
     *      message counts cover the limit.</li>
     *   <li>For ASC: symmetric — start from the oldest, then add newer ones.</li>
     * </ul>

     * @param archives list of archives with [lowerBound, upperBound, messageCount]
     * @param limit number of messages requested
     * @param order ASC (earliest first) or DESC (latest first)
     * @return archives that must be scanned
     */
    private static List<ArchiveMeta> selectTopNArchives(List<ArchiveMeta> archives, long limit, ClpTopNSpec.Order order)
    {
        if (archives == null || archives.isEmpty() || limit <= 0) {
            return ImmutableList.of();
        }

        // 1) Merge overlaps into groups
        List<ArchiveGroup> groups = toArchiveGroups(archives);

        // 2) Pick minimal set of groups per order, then return all member archives
        List<ArchiveMeta> selected = new ArrayList<>();
        if (order == ClpTopNSpec.Order.DESC) {
            // newest group index
            int k = groups.size() - 1;

            // must include newest group
            selected.addAll(groups.get(k).members);

            // assume worst case: newest contributes 0 after filter; cover limit from older groups
            long coveredByOlder = 0;
            for (int i = k - 1; i >= 0 && coveredByOlder < limit; --i) {
                selected.addAll(groups.get(i).members);
                coveredByOlder += groups.get(i).count;
            }
        }
        else {
            // oldest group index
            int k = 0;

            // must include oldest group
            selected.addAll(groups.get(k).members);

            // assume worst case: oldest contributes 0; cover limit from newer groups
            long coveredByNewer = 0;
            for (int i = k + 1; i < groups.size() && coveredByNewer < limit; ++i) {
                selected.addAll(groups.get(i).members);
                coveredByNewer += groups.get(i).count;
            }
        }

        return selected;
    }

    /**
     * Groups overlapping archives into non-overlapping archive groups.
     *
     * @param archives archives sorted by lowerBound
     * @return merged components
     */
    private static List<ArchiveGroup> toArchiveGroups(List<ArchiveMeta> archives)
    {
        List<ArchiveMeta> sorted = new ArrayList<>(archives);
        sorted.sort(comparingLong((ArchiveMeta a) -> a.lowerBound)
                .thenComparingLong(a -> a.upperBound));

        List<ArchiveGroup> groups = new ArrayList<>();
        ArchiveGroup cur = null;

        for (ArchiveMeta a : sorted) {
            if (cur == null) {
                cur = startArchiveGroup(a);
            }
            else if (overlaps(cur, a)) {
                // extend current component
                cur.end = Math.max(cur.end, a.upperBound);
                cur.count += a.messageCount;
                cur.members.add(a);
            }
            else {
                // finalize current, start a new one
                groups.add(cur);
                cur = startArchiveGroup(a);
            }
        }
        if (cur != null) {
            groups.add(cur);
        }
        return groups;
    }

    private static ArchiveGroup startArchiveGroup(ArchiveMeta a)
    {
        ArchiveGroup group = new ArchiveGroup();
        group.begin = a.lowerBound;
        group.end = a.upperBound;
        group.count = a.messageCount;
        group.members.add(a);
        return group;
    }

    private static boolean overlaps(ArchiveGroup cur, ArchiveMeta a)
    {
        return a.lowerBound <= cur.end && a.upperBound >= cur.begin;
    }

    private static List<JsonNode> getQueryResult(URL url, String sql)
    {
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout((int) SECONDS.toMillis(5));
            conn.setReadTimeout((int) SECONDS.toMillis(30));

            log.info("Executing Pinot query: %s", sql);
            ObjectMapper mapper = new ObjectMapper();
            String body = format("{\"sql\": %s }", mapper.writeValueAsString(sql));
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
            if (is == null) {
                throw new IOException("Pinot HTTP " + code + " with empty body");
            }

            JsonNode root;
            try (InputStream in = is) {
                root = mapper.readTree(in);
            }
            JsonNode resultTable = root.get("resultTable");
            if (resultTable == null) {
                throw new RuntimeException("No \"resultTable\" field found");
            }
            JsonNode rows = resultTable.get("rows");
            if (rows == null) {
                throw new RuntimeException("No \"rows\" field found");
            }
            ImmutableList.Builder<JsonNode> resultBuilder = ImmutableList.builder();
            for (Iterator<JsonNode> it = rows.elements(); it.hasNext(); ) {
                JsonNode row = it.next();
                resultBuilder.add(row);
            }
            List<JsonNode> results = resultBuilder.build();
            log.debug("Number of results: %s", results.size());
            return results;
        }
        catch (Exception e) {
            log.error(e);
        }

        return Collections.emptyList();
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

    /**
     * Represents a group of overlapping archives treated as one logical unit.
     */
    private static final class ArchiveGroup
    {
        long begin;
        long end;
        long count;
        final List<ArchiveMeta> members = new ArrayList<>();
    }
}
