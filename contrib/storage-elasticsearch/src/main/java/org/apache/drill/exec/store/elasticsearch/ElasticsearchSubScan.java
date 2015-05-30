/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.elasticsearch.search.SearchOptions;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Elasticsearch sub-scan.
 */
@JsonTypeName("elasticsearch-sub-scan")
public class ElasticsearchSubScan extends AbstractBase implements SubScan {

    private final List<SchemaPath>                     columns;
    private final ElasticsearchStoragePlugin           plugin;
    private final LinkedList<ElasticsearchSubScanSpec> subs;

    public ElasticsearchSubScan(@JacksonInject StoragePluginRegistry registry,
                                @JsonProperty("userName") String userName,
                                @JsonProperty("storage") StoragePluginConfig config,
                                @JsonProperty("nodeScanSpecList") LinkedList<ElasticsearchSubScanSpec> subs,
                                @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
        super(userName);
        this.plugin  = (ElasticsearchStoragePlugin) registry.getPlugin(config);
        this.columns = columns;
        this.subs    = subs;
    }

    public ElasticsearchSubScan(String userName, ElasticsearchStoragePlugin plugin,
                                LinkedList<ElasticsearchSubScanSpec> subs, List<SchemaPath> columns) {
        super(userName);
        this.plugin  = plugin;
        this.columns = columns;
        this.subs    = subs;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new ElasticsearchSubScan(getUserName(), plugin, subs, columns);
    }

    @Override
    public int getOperatorType() {
        return UserBitShared.CoreOperatorType.ELASTICSEARCH_SUB_SCAN_VALUE;
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return Iterators.emptyIterator();
    }

    public List<SchemaPath> getColumns() {
        return columns;
    }

    public List<ElasticsearchSubScanSpec> getSubScanSpecs() {
        return subs;
    }

    @JsonIgnore
    public ElasticsearchStoragePlugin getPlugin() {
        return plugin;
    }

    public static class ElasticsearchSubScanSpec {

        private String schema;
        private String table;
        private String query;
        private SearchOptions options;

        @parquet.org.codehaus.jackson.annotate.JsonCreator
        public ElasticsearchSubScanSpec(@JsonProperty("schema") String schema,
                                        @JsonProperty("table") String table,
                                        @JsonProperty("query") String query,
                                        @JsonProperty("options") SearchOptions options) {
            this.schema  = schema;
            this.table   = table;
            this.query   = query;
            this.options = options;
        }

        @Override
        public String toString() {
            return "ElasticsearchSubScanSpec{" +
                    "schema='" + schema + '\'' +
                    ", table='" + table + '\'' +
                    ", options='" + options.toString() + '\'' +
                    ", query=" + query + '\'' +
                    '}';
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public SearchOptions getOptions() {
            return options;
        }

        public void setOptions() {
            this.options = options;
        }
    }
}
