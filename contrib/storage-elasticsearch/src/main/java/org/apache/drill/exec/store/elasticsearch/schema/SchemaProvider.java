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
package org.apache.drill.exec.store.elasticsearch.schema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.elasticsearch.types.ElasticsearchType;
import org.apache.drill.exec.store.elasticsearch.UnsupportedTypeException;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePluginConfig;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable.TableDefinition;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable.ColumnDefinition;

import org.elasticsearch.client.Client;
//import org.elasticsearch.common.hppc.ObjectLookupContainer;
//import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * Provider for schema and table definitions.
 */
public class SchemaProvider {

    private static final Logger logger = LoggerFactory.getLogger(SchemaProvider.class);

    private final Client client;

    public SchemaProvider(ElasticsearchStoragePluginConfig config) {
        this.client = config.getClient();
    }

    public SchemaProvider(Client client) {
        this.client = client;
    }

    /**
     * Gets all schemas.
     */
    public List<String> schemas() {
        try {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            IndicesStatsResponse response = client.admin().indices().prepareStats().execute().actionGet();
            return builder.addAll(response.getIndices().keySet()).build();
        }
        catch (Exception e) {
            logger.warn("Failure while loading schemas", e);
            return ImmutableList.of();
        }
    }

    /**
     * Gets all tables in the given schema.
     */
    public Set<String> tables(String schema) {

        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(schema).execute().actionGet();

        if (response.mappings().size() == 0) {
            return ImmutableSet.of();
        }

        for (ObjectCursor<ImmutableOpenMap<String, MappingMetaData>> cursor : response.mappings().values()) {
            ObjectLookupContainer<String> keys = cursor.value.keys();
            for (ObjectCursor<String> key : keys) {
                builder.add(key.value);
            }
        }

        return builder.build();
    }

    /**
     * Loads the table definition for the given table.
     */
    public TableDefinition table(String schema, String table) throws IOException, UnsupportedTypeException {

        logger.info("Reading table definition: [{}.{}]", schema, table);

        List<ColumnDefinition> columns = null;

        final GetMappingsResponse response =
                client.admin().indices().prepareGetMappings(schema).setTypes(table).execute().actionGet();

        if (response.mappings().isEmpty()) {
            throw new DrillRuntimeException(format("No definition exists for table: [%s.%s]", schema, table));
        }
        else if (response.mappings().size() > 1) {
            throw new DrillRuntimeException(format("Multiple definitions exist for table: [%s.%s]. " +
                    "Wildcards and aliases are currently not supported.", schema, table));
        }

        ImmutableOpenMap<String, MappingMetaData> metadata = response.mappings().get(schema);

        // Metadata can be null if the given schema name is an alias.
        if (metadata == null) {
            throw new DrillRuntimeException(format("No definition exists for table: [%s.%s]. " +
                    "If using an alias, please switch to the concrete table name.", schema, table));
        }

        for (ObjectCursor<MappingMetaData> metadataCursor : metadata.values()) {
            Map<String, Object> map = metadataCursor.value.sourceAsMap();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                columns = mappings((Map<String, Map>) entry.getValue());
                break;
            }
            break;
        }

        if (columns == null || columns.size() == 0) {
            throw new DrillRuntimeException(format("Table [%s.%s] has no columns", schema, table));
        }

        return new TableDefinition(schema, table, true, columns);
    }

    /**
     * Creates a mapping of field names to field types.
     * <p>
     * For each field in the elasticsearch index this method infers the appropriate
     * internal Drill data type to use.
     * </p>
     */
    private List<ColumnDefinition> mappings(Map<String, Map> map, String... parents) throws UnsupportedTypeException {

        ImmutableList.Builder<ColumnDefinition> builder = ImmutableList.builder();

        for (Map.Entry<String, Map> entry : map.entrySet()) {

            String fieldName = entry.getKey();
            String fieldType = (String) entry.getValue().get("type");

            if (fieldType.equalsIgnoreCase("nested") || fieldType.equalsIgnoreCase("object")) {

                @SuppressWarnings("unchecked")
                List<ColumnDefinition> nested =
                        mappings((Map) entry.getValue().get("properties"), compound(fieldName, parents));

                ColumnDefinition cd =
                        new ColumnDefinition(fieldName, SchemaPath.getSimplePath(fieldName), ElasticsearchType.NESTED);

                for (ColumnDefinition col : nested) {
                    cd.addChild(col);
                }

                builder.add(cd);
            }
            else {
                ElasticsearchType type = ElasticsearchType.of(fieldType);
                if (type == null) {
                    throw new UnsupportedTypeException(fieldName, fieldType);
                }

                SchemaPath sp = SchemaPath.getCompoundPath(compound(fieldName, parents));
                builder.add(new ColumnDefinition(fieldName, sp, type));
            }
        }

        return builder.build();
    }

    private String[] compound(String s, String...parents) {
        if (parents == null || parents.length == 0) {
            return new String[] { s };
        }
        String[] sa = Arrays.copyOf(parents, parents.length + 1);
        sa[sa.length - 1] = s;
        return sa;
    }
}