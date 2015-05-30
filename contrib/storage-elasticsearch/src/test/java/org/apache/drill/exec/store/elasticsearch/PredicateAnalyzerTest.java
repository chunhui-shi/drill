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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.exec.store.elasticsearch.random.RandomizedTestBase;
import org.apache.drill.exec.store.elasticsearch.rules.ExpressionNotAnalyzableException;
import org.apache.drill.exec.store.elasticsearch.rules.PredicateAnalyzer;

import org.elasticsearch.common.bytes.BytesReference;

import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Unit tests for predicate analysis.
 */
@Repeat(iterations = 10, useConstantSeed = false)
public class PredicateAnalyzerTest extends RandomizedTestBase {

    private static final Logger logger = LoggerFactory.getLogger(PredicateAnalyzerTest.class);

    private static final RelDataTypeFactory factory =
            new SqlTypeFactoryImpl(DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM);

    private static final RexBuilder builder = new RexBuilder(factory);

    private static final RelDataType INTEGER = factory.createSqlType(SqlTypeName.INTEGER);
    private static final RelDataType BIGINT  = factory.createSqlType(SqlTypeName.BIGINT);
    private static final RelDataType FLOAT   = factory.createSqlType(SqlTypeName.FLOAT);
    private static final RelDataType DOUBLE  = factory.createSqlType(SqlTypeName.DOUBLE);
    private static final RelDataType BOOLEAN = factory.createSqlType(SqlTypeName.BOOLEAN);
    private static final RelDataType VARCHAR = factory.createSqlType(SqlTypeName.VARCHAR);
    private static final RelDataType MAP     = factory.createMapType(VARCHAR, factory.createSqlType(SqlTypeName.ANY));

    private static final List<Field> primitives = primitives();

    /** Equality Tests **/

    @Test
    public void equals() throws Exception {

        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        RexNode call = eq(shuffle(
                builder.makeInputRef(field.type(), field.index),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing equality: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = termQuery(field.name, value).buildAsBytes();
        BytesReference actual   = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void equals_Literal_Literal() throws Exception {

        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        // Expect failure: comparing two literals does not make sense
        RexNode call = eq(shuffle(
                builder.makeLiteral(value, field.type(), randomBoolean()),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing equality: {}", call);
        PredicateAnalyzer.analyze(input(), call);
        fail();
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void equals_InputRef_InputRef() throws Exception {

        Field f1 = randomFrom(primitives);
        Field f2 = randomFrom(primitives);

        // Expect failure: comparing two fields is not supported
        RexNode call = eq(shuffle(
                builder.makeInputRef(f1.type(), f1.index), builder.makeInputRef(f2.type(), f2.index)));

        logger.info("--> testing equality: {}", call);
        PredicateAnalyzer.analyze(input(), call);
    }

    /** Nested Equality Tests **/

    @Test
    public void nestedEquals() throws Exception {

        String nestedFieldName = "a";
        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        RexNode call = eq(shuffle(
                item(builder.makeInputRef(MAP, 6), builder.makeLiteral(nestedFieldName, VARCHAR, false)),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing nested equality: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = nestedQuery("map_field", termQuery("map_field." + nestedFieldName, value)).buildAsBytes();
        BytesReference actual   = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void nestedEquals_InputRef_InputRef() throws Exception {

        Field f1 = randomFrom(primitives);
        Field f2 = randomFrom(primitives);

        // Expect failure: comparing two fields is not supported
        RexNode call = eq(
                item(builder.makeInputRef(MAP, 6), builder.makeLiteral(f1.randomValue(), f1.type(), randomBoolean())),
                item(builder.makeInputRef(MAP, 6), builder.makeLiteral(f2.randomValue(), f2.type(), randomBoolean())));

        logger.info("--> testing nested equality: {}", call);
        PredicateAnalyzer.analyze(input(), call);
    }

    /** Non-equality Tests **/

    @Test
    public void notEquals() throws Exception {

        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        RexNode call = not(shuffle(
                builder.makeInputRef(field.type(), field.index),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing non-equality: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = notQuery(termQuery(field.name, value)).buildAsBytes();
        BytesReference actual   = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void notEquals_Literal_Literal() throws Exception {

        // Expect failure: comparing two literals does not make sense
        RexNode call = not(shuffle(
                builder.makeLiteral(5, INTEGER, true), builder.makeLiteral(5, INTEGER, true)));

        logger.info("--> testing non-equality: {}", call);
        PredicateAnalyzer.analyze(input(), call);
        fail();
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void notEquals_InputRef_InputRef() throws Exception {

        // Expect failure: comparing two fields is not supported
        RexNode call = not(shuffle(
                builder.makeInputRef(INTEGER, 0), builder.makeInputRef(INTEGER, 0)));

        logger.info("--> testing non-equality: {}", call);
        PredicateAnalyzer.analyze(input(), call);
        fail();
    }

    /** Nested Non-equality Tests **/

    @Test
    public void nestedNotEquals() throws Exception {

        String nestedFieldName = "a";
        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        RexNode call = not(shuffle(
                item(builder.makeInputRef(MAP, 6), builder.makeLiteral(nestedFieldName, VARCHAR, false)),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing nested non-equality: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = nestedQuery("map_field", notQuery(termQuery("map_field." + nestedFieldName, value))).buildAsBytes();
        BytesReference actual   = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    /** Inequality Tests **/

    @Test
    public void greaterThan() throws Exception {

        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        RexNode call = gt(shuffle(
                builder.makeInputRef(field.type(), field.index),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing greater than: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = rangeQuery(field.name).gt(value).buildAsBytes();
        BytesReference actual   = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void greaterThan_Literal_Literal() throws Exception {

        // Expect failure: comparing two literals does not make sense
        RexNode call = gt(shuffle(
                builder.makeLiteral(6, INTEGER, true),
                builder.makeLiteral(5, INTEGER, true)));

        logger.info("--> testing greater than: {}", call);
        PredicateAnalyzer.analyze(input(), call);
        fail();
    }

    @Test(expected = ExpressionNotAnalyzableException.class)
    public void greaterThan_InputRef_InputRef() throws Exception {

        // Expect failure: comparing two fields is not supported
        RexNode call = gt(
                builder.makeInputRef(INTEGER, 0),
                builder.makeInputRef(INTEGER, 0));

        logger.info("--> testing greater than: {}", call);
        PredicateAnalyzer.analyze(input(), call);
        fail();
    }

    @Test
    public void lessThan() throws Exception {

        Field field = randomFrom(primitives);
        Object value = field.randomValue();

        RexNode call = lt(shuffle(
                builder.makeInputRef(field.type(), field.index),
                builder.makeLiteral(value, field.type(), randomBoolean())));

        logger.info("--> testing less than: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = rangeQuery(field.name).lt(value).buildAsBytes();
        BytesReference actual   = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    /** Boolean OR Tests **/

    @Test
    public void or() throws Exception {

        Field field1 = randomFrom(primitives);
        Object value1 = field1.randomValue();
        Field field2 = randomFrom(primitives);
        Object value2 = field2.randomValue();

        RexNode call = or(
                gte(shuffle(builder.makeInputRef(field1.type(), field1.index),
                        builder.makeLiteral(value1, field1.type(), randomBoolean()))),
                eq(shuffle(builder.makeInputRef(field2.type(), field2.index),
                        builder.makeLiteral(value2, field2.type(), randomBoolean())))
        );

        logger.info("--> testing 'or' operator: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = boolQuery().should(rangeQuery(field1.name).gte(value1)).should(termQuery(field2.name, value2)).buildAsBytes();
        BytesReference actual = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    /** Boolean AND Tests **/

    @Test
    public void and() throws Exception {

        Field field1 = randomFrom(primitives);
        Object value1 = field1.randomValue();
        Field field2 = randomFrom(primitives);
        Object value2 = field2.randomValue();

        RexNode call = and(
                eq(shuffle(builder.makeInputRef(field1.type(), field1.index),
                        builder.makeLiteral(value1, field1.type(), randomBoolean()))),
                lt(shuffle(builder.makeInputRef(field2.type(), field2.index),
                        builder.makeLiteral(value2, field2.type(), randomBoolean())))
        );

        logger.info("--> testing 'and' operator: {}", call);
        PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input(), call);

        BytesReference expected = boolQuery().must(termQuery(field1.name, value1)).must(rangeQuery(field2.name).lt(value2)).buildAsBytes();
        BytesReference actual = analyzed.builder().buildAsBytes();

        assertEquals(expected, actual);
    }

    /** *** Utilities *** **/

    private static RexNode and(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.AND, nodes);
    }

    private static RexNode or(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.OR, nodes);
    }

    private static RexNode eq(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.EQUALS, nodes);
    }

    private static RexNode not(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, nodes);
    }

    private static RexNode gt(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.GREATER_THAN, nodes);
    }

    private static RexNode gte(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, nodes);
    }

    private static RexNode lt(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.LESS_THAN, nodes);
    }

    private static RexNode lte(RexNode... nodes) {
        return builder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, nodes);
    }

    private static RexNode item(RexNode left, RexNode right) {
        return builder.makeCall(SqlStdOperatorTable.ITEM, left, right);
    }

    private static RexNode[] shuffle(RexNode left, RexNode right) {
        return randomBoolean() ? new RexNode[] { left, right } : new RexNode[] { right, left };
    }

    public static final class Field {

        final String           name;
        final int              index;
        final RelDataTypeField type;

        Field(String name, int index, RelDataTypeField type) {
            this.name  = name;
            this.index = index;
            this.type  = type;
        }

        RelDataType type() {
            return type.getType();
        }

        public Object randomValue() {
            switch (type.getType().getSqlTypeName().getName()) {
                case "INTEGER":
                    return randomInt();
                case "BIGINT":
                    return randomLong();
                case "FLOAT":   // Fall through
                case "DOUBLE":
                    return randomDouble();
                case "BOOLEAN":
                    return randomBoolean();
                case "VARCHAR":
                    return randomAscii();
                default:
                    return null;
            }
        }
    }

    private static List<Field> primitives() {

        ImmutableList.Builder<Field> builder = ImmutableList.builder();

        builder.add(new Field("integer_field", 0, new RelDataTypeFieldImpl("integer_field", 0, INTEGER)));
        builder.add(new Field("long_field",    1, new RelDataTypeFieldImpl("long_field", 1, BIGINT)));
        builder.add(new Field("float_field",   2, new RelDataTypeFieldImpl("float_field", 2, FLOAT)));
        builder.add(new Field("double_field",  3, new RelDataTypeFieldImpl("double_field", 3, DOUBLE)));
        builder.add(new Field("boolean_field", 4, new RelDataTypeFieldImpl("boolean_field", 4, BOOLEAN)));
        builder.add(new Field("string_field",  5, new RelDataTypeFieldImpl("string_field", 5, VARCHAR)));

        return builder.build();
    }

    private static List<Field> fields() {

        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        builder.addAll(primitives());
        builder.add(new Field("map_field", 6, new RelDataTypeFieldImpl("map_field", 6, MAP)));

        return builder.build();
    }

    private static RelNode input() {

        RelNode input = mock(RelNode.class);
        RelRecordType record = record();
        when(input.getRowType()).thenReturn(record);

        return input;
    }

    private static RelRecordType record() {

        List<RelDataTypeField> fields = new ArrayList<>();
        for (Field field : fields()) {
            fields.add(field.type);
        }
        return new RelRecordType(fields);
    }
}
