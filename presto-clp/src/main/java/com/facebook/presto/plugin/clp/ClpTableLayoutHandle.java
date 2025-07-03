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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ClpTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ClpTableHandle table;
    private final Optional<String> metadataFilterQuery;

    private Optional<String> kqlQuery;

    @JsonCreator
    public ClpTableLayoutHandle(@JsonProperty("table") ClpTableHandle table,
                                @JsonProperty("kqlQuery") Optional<String> kqlQuery,
                                @JsonProperty("metadataFilterQuery") Optional<String> metadataFilterQuery)
    {
        this.table = table;
        this.kqlQuery = kqlQuery;
        this.metadataFilterQuery = metadataFilterQuery;
    }

    @JsonProperty
    public ClpTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<String> getKqlQuery()
    {
        return kqlQuery;
    }

    @JsonProperty
    public void setKqlQuery(String kqlQuery)
    {
        this.kqlQuery = Optional.of(kqlQuery);
    }

    @JsonProperty
    public Optional<String> getMetadataFilterQuery()
    {
        return metadataFilterQuery;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClpTableLayoutHandle that = (ClpTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(kqlQuery, that.kqlQuery) &&
                Objects.equals(metadataFilterQuery, that.metadataFilterQuery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, kqlQuery, metadataFilterQuery);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("kqlQuery", kqlQuery)
                .add("metadataFilterQuery", metadataFilterQuery)
                .toString();
    }
}
