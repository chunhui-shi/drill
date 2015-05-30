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

import org.junit.Before;
import org.junit.Test;

import org.apache.drill.exec.store.elasticsearch.util.ClusterUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestElasticsearchProjectPushDown extends ElasticsearchTestBase {

    private static final Logger logger = LoggerFactory.getLogger(TestElasticsearchProjectPushDown.class);

    private static final int MAX_DOCS = 25;
    private int ndocs = 0;

    @Before
    public void before() {
        ndocs = random().nextInt(MAX_DOCS);
    }

    @Test
    public void selectStar_PrimitiveTypes() throws Exception {

        logger.info("--> testing select star : primitive types");

        cluster.populate("test_schema_1", "test_table_1", ndocs, ClusterUtil.PRIMITIVE_TYPES);

        String sql = "select * from elasticsearch.`test_schema_1`.test_table_1";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_PrimitiveTypes() throws Exception {

        logger.info("--> testing select named columns : primitive types");

        cluster.populate("test_schema_2", "test_table_2", ndocs, ClusterUtil.PRIMITIVE_TYPES);

        String sql = "select string_field, integer_field, long_field, float_field, " +
                " double_field, boolean_field from elasticsearch.`test_schema_2`.test_table_2";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_SingleLevelNestedTypes() throws Exception {

        logger.info("--> testing select named columns : single-level nested types");

        cluster.populate("test_schema_3", "test_table_3", ndocs, ClusterUtil.ALL_TYPES);

        String sql = "select " +
                " person['ssn'] as person_ssn," +
                " person['first_name'] as person_first_name," +
                " person['last_name'] as person_last_name " +
                " from elasticsearch.`test_schema_3`.test_table_3";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_SingleLevelObjectType() throws Exception {

        logger.info("--> testing select named columns : single-level object type");

        cluster.populate("test_schema_3", "test_table_3", ndocs, ClusterUtil.ALL_TYPES);

        String sql = "select " +
                " person2['ssn'] as person2_ssn," +
                " person2['first_name'] as person2_first_name," +
                " person2['last_name'] as person2_last_name " +
                " from elasticsearch.`test_schema_3`.test_table_3";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_DoubleLevelNestedTypes() throws Exception {

        logger.info("--> testing select named columns : double-level nested types");

        cluster.populate("test_schema_4", "test_table_4", ndocs, ClusterUtil.ALL_TYPES);

        String sql = "select " +
                " person['ssn'] as person_ssn," +
                " person['first_name'] as person_first_name," +
                " person['last_name'] as person_last_name, " +
                " person['address']['city'] as person_address_city, " +
                " person['address']['zipcode'] as person_address_zipcode, " +
                " person['relative']['last_name'] as person_relative_last_name " +
                " from elasticsearch.`test_schema_4`.test_table_4";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectStar_SimpleWhereClause() throws Exception {

        logger.info("--> testing select star : simple where clause");

        cluster.populate("test_schema_5", "test_table_5", ndocs, ClusterUtil.ALL_TYPES);

        String sql = "select * from " +
                " elasticsearch.`test_schema_5`.test_table_5 " +
                " where integer_field > 1";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_SimpleWhereClause() throws Exception {

        logger.info("--> testing select named columns : simple where clause");

        cluster.populate("test_schema_5", "test_table_5", ndocs, ClusterUtil.ALL_TYPES);

        String sql = "select " +
                " integer_field, string_field from " +
                " elasticsearch.`test_schema_5`.test_table_5 " +
                " where integer_field > 1";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_ComplexWhereClause() throws Exception {

        logger.info("--> testing select star : complex where clause");

        cluster.populate("test_schema_6", "test_table_6", ndocs, ClusterUtil.PRIMITIVE_TYPES);

        String sql = "select " +
                " integer_field, string_field " +
                " from elasticsearch.`test_schema_6`.test_table_6 where " +
                " (integer_field > 0 AND integer_field < 20) " +
                " OR string_field = 'string_value_21' " +
                " OR string_field = 'string_value_23'";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectNamed_SimpleWhereClauseWithNestedTypes() throws Exception {

        logger.info("--> testing select named : simple where clause : nested types");

        cluster.populate("test_schema_7", "test_table_7", ndocs, ClusterUtil.ALL_TYPES);

        String sql = "select " +
                " string_field, boolean_field, " +
                " person['first_name'] as person_first_name, " +
                " person['ssn'] as person_ssn, " +
                " person['address']['city'] as person_address_city " +
                " from elasticsearch.`test_schema_7`.test_table_7 where " +
                " person['ssn'] <> 1237 ";
                //" person['address']['city'] = 'seattle'";
                //" integer_field = 5 ";
                //" 7 = integer_field ";
                //" OR person['last_name'] = 'my_last_name_11'";

        display(testSqlWithResults(sql));
    }

    @Test
    public void selectStar_ArrayValues() throws Exception {

        logger.info("--> testing select named : simple where clause : array values");

        cluster.populate("test_schema_8", "test_table_8", ndocs, true, ClusterUtil.ALL_TYPES);

        String sql = "select * from elasticsearch.`test_schema_8`.test_table_8";
        display(testSqlWithResults(sql));
    }
}
