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

import org.junit.Test;

import org.apache.drill.exec.store.elasticsearch.util.ClusterUtil;
import org.apache.drill.exec.store.elasticsearch.schema.SchemaProvider;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable.TableDefinition;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;


/**
 *
 */
public class TestSchemaProvider extends ElasticsearchTestBase {

    private static final Logger logger = LoggerFactory.getLogger(TestSchemaProvider.class);

    @Test
    public void testGetSchemas() throws Exception {

        cluster.schema("test_schema_x", "test_schema_y", "test_schema_z");
        SchemaProvider provider = new SchemaProvider(cluster.config());
        List<String> schemas = provider.schemas();
        assertThat(schemas.size(), equalTo(3));
    }

    @Test
    public void testGetTables() throws Exception {

        cluster.table("test_schema_2", "test_table_1", "test_table_2");

        SchemaProvider provider = new SchemaProvider(cluster.config());
        Set<String> tables = provider.tables("test_schema_2");

        assertThat(tables.size(), equalTo(2));
        assertTrue(tables.contains("test_table_1"));
        assertTrue(tables.contains("test_table_2"));
    }

    @Test
    public void testGetTable() throws Exception {

        cluster.table("test_schema_3", "test_table_1");

        SchemaProvider provider = new SchemaProvider(cluster.config());
        TableDefinition table = provider.table("test_schema_3", "test_table_1");

        assertNotNull(table);
        assertEquals("test_schema_3", table.schema());
        assertEquals("test_table_1", table.name());

        logger.info("--> table: {}", table);
    }

    @Test(expected = org.apache.drill.common.exceptions.DrillRuntimeException.class)
    public void testNoSuchTable() throws Exception {

        cluster.schema("test_schema_4");
        SchemaProvider provider = new SchemaProvider(cluster.config());
        provider.table("test_schema_4", "test_table");
    }

    @Test(expected = org.apache.drill.common.exceptions.DrillRuntimeException.class)
    public void testAliasToMultipleTables() throws Exception {

        cluster.table("test_schema_abc", "table_with_same_name_in_two_schemas");
        cluster.table("test_schema_def", "table_with_same_name_in_two_schemas");
        cluster.alias("all_test_schemas", "test_schema_abc", "test_schema_def");

        SchemaProvider provider = new SchemaProvider(cluster.config());
        provider.table("all_test_schemas", "table_with_same_name_in_two_schemas");
    }

    @Test(expected = org.apache.drill.common.exceptions.DrillRuntimeException.class)
    public void testAliasToSingleTable() throws Exception {

        cluster.table("test_schema_5", "test_table_1");
        cluster.alias("test_alias", "test_schema_5");

        SchemaProvider provider = new SchemaProvider(cluster.config());
        provider.table("test_alias", "test_table_1");
    }

    @Test
    public void testGetTableWithNestedColumns() throws Exception {

        cluster.table("test_schema_nested", "test_table_1", ClusterUtil.NESTED_TYPES);

        SchemaProvider provider = new SchemaProvider(cluster.config());
        TableDefinition table = provider.table("test_schema_nested", "test_table_1");

        assertNotNull(table);
        assertEquals("test_schema_nested", table.schema());
        assertEquals("test_table_1", table.name());

        logger.info("--> table: {}", table);
    }
}
