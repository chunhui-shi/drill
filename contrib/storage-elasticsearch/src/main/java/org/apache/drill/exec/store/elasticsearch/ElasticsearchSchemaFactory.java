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

import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.index.IndexDiscoverable;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable.TableDefinition;
import org.apache.drill.exec.store.elasticsearch.schema.SchemaProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

/**
 * Schema factory for elasticsearch
 */
public class ElasticsearchSchemaFactory implements SchemaFactory, IndexDiscoverable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSchemaFactory.class);

    private final String schema;
    private final SchemaProvider provider;
    private final ElasticsearchStoragePlugin plugin;

    public ElasticsearchSchemaFactory(ElasticsearchStoragePlugin plugin, String schema) {
        this.plugin   = plugin;
        this.schema   = schema;
        this.provider = new SchemaProvider(plugin.getConfig());

        logger.info("Factory created for elasticsearch storage plugin: [{}]", schema);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        ElasticsearchSchema es = new ElasticsearchSchema(schema);
        SchemaPlus plus = parent.add(schema, es);
        es.setHolder(plus);

        logger.info("Schema registered for elasticsearch storage plugin: [{}]", schema);
    }

    @Override
    public DrillTable findTable(List<String> names) {
        //XXX now assume the input names is <storage>.<schema>.<table>
        if(names.size() < 1 ) {
            logger.warn("not enough information for finding a table: " + names.toString());
            return null;
        }
        if (! names.get(0).equalsIgnoreCase(this.schema)){
            logger.warn("root schema name not match between {} AND {}", names.get(0), this.schema);
            return null;
        }

        String schemaName = names.get(1);
        String tableName = names.get(2);
        try {
            TableDefinition def = provider.table(schemaName, tableName);
            ElasticsearchScanSpec spec = new ElasticsearchScanSpec(schemaName, tableName, null);
            return new DrillElasticsearchTable(def, schema, plugin, spec);
        }
        catch (IOException | UnsupportedTypeException e) {
            throw new DrillRuntimeException(format("Failed to read table: [%s.%s]", schemaName, tableName), e);
        }

    }

    /**
     * Representation of an elasticsearch index as a drill schema.
     *
     * Each document type within the index is represented as a table.
     * For an explanation of elasticsearch document types:
     * @see "https://www.elastic.co/guide/en/elasticsearch/reference/current/_basic_concepts.html#_type"
     */
    private class ElasticsearchSchema extends AbstractSchema {

        public ElasticsearchSchema(String name) {
            super(ImmutableList.<String>of(), name);
        }

        public void setHolder(SchemaPlus plusOfThis) {
            for (String s : getSubSchemaNames()) {
                plusOfThis.add(s, getSubSchema(s));
            }
        }

        @Override
        public AbstractSchema getSubSchema(String name) {

            List<String> schemas = provider.schemas();
            if (!schemas.contains(name)) {
                logger.info("Schema: [{}] does not exist in elasticsearch", name);
                return null;
            }

            Set<String> tables = provider.tables(name);
            return new ElasticsearchDatabaseSchema(this, name, tables);
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Sets.newHashSet(provider.schemas());
        }

        @Override
        public Set<String> getTableNames() {
            return provider.tables(schema);
        }

        @Override
        public String getTypeName() {
            return ElasticsearchStoragePluginConfig.NAME;
        }
    }

    public class ElasticsearchDatabaseSchema extends AbstractSchema {

        private final Set<String> tables;

        public ElasticsearchDatabaseSchema(ElasticsearchSchema es, String name, Set<String> tables) {
            super(es.getSchemaPath(), name);
            this.tables = tables;
        }

        @Override
        public String getTypeName() {
            return ElasticsearchStoragePluginConfig.NAME;
        }

        @Override
        public Table getTable(final String table) {

            try {
                TableDefinition def = provider.table(name, table);
                ElasticsearchScanSpec spec = new ElasticsearchScanSpec(name, table, null);
                return new DrillElasticsearchTable(def, schema, plugin, spec);
            }
            catch (IOException | UnsupportedTypeException e) {
                throw new DrillRuntimeException(format("Failed to read table: [%s.%s]", name, table), e);
            }
        }

        @Override
        public Set<String> getTableNames() {
            return tables;
        }
    }
}
