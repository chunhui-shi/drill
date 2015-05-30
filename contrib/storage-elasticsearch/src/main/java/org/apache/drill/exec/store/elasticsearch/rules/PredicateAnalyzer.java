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
package org.apache.drill.exec.store.elasticsearch.rules;

import com.google.common.base.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Query predicate analyzer.
 */
public class PredicateAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(PredicateAnalyzer.class);

    /**
     * Walks the expression tree, attempting to convert the entire tree into
     * an equivalent elasticsearch query filter. If an error occurs, or if it
     * is determined that the expression cannot be converted, an exception is
     * thrown and an error message logged.
     *
     * Callers should catch ExpressionNotAnalyzableException and fall back to not using push-down filters.
     */
    public static QueryExpression analyze(RelNode input, RexNode expression) throws ExpressionNotAnalyzableException {
        try {
            QueryExpression e = (QueryExpression) expression.accept(new Visitor(input));
            logger.debug("Predicate: [{}] converted to: [\n{}]", expression, queryAsJson(e.builder()));
            return e;
        }
        catch (Throwable e) {
            throw new ExpressionNotAnalyzableException(format(
                    "Failed to fully convert predicate: [%s] into an elasticsearch filter", expression), e);
        }
    }

    private static class Visitor extends RexVisitorImpl<Expression> {

        private final RelNode input;

        protected Visitor(RelNode input) {
            super(true);
            this.input = input;
        }

        @Override
        public Expression visitInputRef(RexInputRef inputRef) {
            return new NamedFieldExpression(input.getRowType().getFieldList().get(inputRef.getIndex()));
        }

        @Override
        public Expression visitLocalRef(RexLocalRef localRef) {
            return super.visitLocalRef(localRef);
        }

        @Override
        public Expression visitLiteral(RexLiteral literal) {
            return new LiteralExpression(literal);
        }

        @Override
        public Expression visitOver(RexOver over) {
            return super.visitOver(over);
        }

        @Override
        public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
            return super.visitCorrelVariable(correlVariable);
        }

        @Override
        public Expression visitCall(RexCall call) {

            SqlSyntax syntax = call.getOperator().getSyntax();

            switch (syntax) {
                case BINARY:
                    return binary(call);
                case PREFIX:
                    return prefix(call);
                case POSTFIX:
                    return postfix(call);
                case SPECIAL:
                    switch (call.getKind()) {
                        case CAST:
                            return cast(call);
                        case CASE:
                        case LIKE:
                        case SIMILAR:
                            throw new DrillRuntimeException(format("Unsupported operator: [%s]", call));
                    }

                    if (call.getOperator() == SqlStdOperatorTable.ITEM) {

                        RexNode operand    = call.getOperands().get(0);
                        RexLiteral literal = (RexLiteral) call.getOperands().get(1);

                        if (operand instanceof RexInputRef) {
                            RelDataTypeField field =
                                    input.getRowType().getFieldList().get(((RexInputRef) operand).getIndex());
                            return new NestedNamedFieldExpression(field, literal);
                        }
                        else if (operand instanceof RexCall) {
                            // Multiple layers of nesting encountered; recurse until we get to the bottom.
                            NestedNamedFieldExpression parent = (NestedNamedFieldExpression) operand.accept(this);
                            return new NestedNamedFieldExpression(parent, literal);
                        }
                    }

                    throw new DrillRuntimeException(format("Unsupported syntax [%s] for call: [%s]", syntax, call));
                case FUNCTION:
                case FUNCTION_ID:
                case FUNCTION_STAR:
                    throw new DrillRuntimeException(format("Unsupported operator: [%s]", call));
                default:
                    throw new DrillRuntimeException(format("Unsupported syntax [%s] for call: [%s]", syntax, call));
            }
        }

        @Override
        public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
            return super.visitDynamicParam(dynamicParam);
        }

        @Override
        public Expression visitRangeRef(RexRangeRef rangeRef) {
            return super.visitRangeRef(rangeRef);
        }

        @Override
        public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
            return super.visitFieldAccess(fieldAccess);
        }

        private CastExpression cast(RexCall call) {

            LiteralExpression argument = (LiteralExpression) call.getOperands().get(0).accept(this);
            MajorType target;

            switch (call.getType().getSqlTypeName()) {

                case CHAR:
                case VARCHAR:
                    target = Types.required(MinorType.VARCHAR)
                                .toBuilder().setWidth(call.getType().getPrecision()).build();
                    break;
                case INTEGER:
                    target = Types.required(MinorType.INT);
                    break;
                case FLOAT:
                    target = Types.required(MinorType.FLOAT4);
                    break;
                case DOUBLE:
                    target = Types.required(MinorType.FLOAT8);
                    break;
                case DECIMAL:
                    throw new DrillRuntimeException("Cast to DECIMAL type unsupported");
                default:
                    target = Types.required(MinorType.valueOf(call.getType().getSqlTypeName().getName()));
            }

            return new CastExpression(target, argument);
        }

        private QueryExpression postfix(RexCall call) {

            switch (call.getKind()) {
                case IS_NOT_NULL:
                case IS_NOT_TRUE:
                case IS_NOT_FALSE:
                case IS_NULL:
                case IS_TRUE:
                case IS_FALSE:
                case OTHER:
                default:
                    throw new DrillRuntimeException(format("Unsupported operator: [%s]", call));
            }
        }

        private QueryExpression prefix(RexCall call) {

            switch (call.getKind()) {
                case NOT:
                default:
                    throw new DrillRuntimeException(format("Unsupported operator: [%s]", call));
            }
        }

        private QueryExpression binary(RexCall call) {

            Preconditions.checkState(call.getOperands().size() == 2);

            Expression a = call.getOperands().get(0).accept(this);
            Expression b = call.getOperands().get(1).accept(this);

            if (SqlKind.COMPARISON.contains(call.getKind())) {

                Pair<TerminalExpression, LiteralExpression> pair = swap(a, b);

                switch (call.getKind()) {
                    case EQUALS:
                        return QueryExpression.create(pair.getKey()).equals(pair.getValue());
                    case NOT_EQUALS:
                        return QueryExpression.create(pair.getKey()).not(pair.getValue());
                    case GREATER_THAN:
                        return QueryExpression.create(pair.getKey()).gt(pair.getValue());
                    case GREATER_THAN_OR_EQUAL:
                        return QueryExpression.create(pair.getKey()).gte(pair.getValue());
                    case LESS_THAN:
                        return QueryExpression.create(pair.getKey()).lt(pair.getValue());
                    case LESS_THAN_OR_EQUAL:
                        return QueryExpression.create(pair.getKey()).lte(pair.getValue());
                    default:
                        throw new DrillRuntimeException(format("Unable to handle call: [%s]", call));
                }
            }
            else {
                switch (call.getKind()) {
                    case OR:
                        return CompoundQueryExpression.or((QueryExpression) a, (QueryExpression) b);
                    case AND:
                        return CompoundQueryExpression.and((QueryExpression) a, (QueryExpression) b);
                    default:
                        throw new DrillRuntimeException(format("Unable to handle call: [%s]", call));
                }
            }
        }

        private static Pair<TerminalExpression, LiteralExpression> swap(Expression left, Expression right) {

            Preconditions.checkState(
                    (left instanceof LiteralExpression ^ right instanceof LiteralExpression) ||
                    (left instanceof CastExpression ^ right instanceof CastExpression)
            );

            if (left instanceof LiteralExpression) {
                return new Pair<>((TerminalExpression) right, (LiteralExpression) left);
            }
            else if (left instanceof CastExpression) {
                return new Pair<>((TerminalExpression) right, ((CastExpression) left).argument);
            }
            else if (right instanceof CastExpression) {
                return new Pair<>((TerminalExpression) left, ((CastExpression) right).argument);
            }
            return new Pair<>((TerminalExpression) left, (LiteralExpression) right);
        }
    }

    /** Empty interface; exists only to define type hierarchy */
    public interface Expression { }

    public abstract static class QueryExpression implements Expression {

        public abstract QueryBuilder builder();

        public abstract QueryExpression equals(LiteralExpression literal);
        public abstract QueryExpression not(LiteralExpression literal);
        public abstract QueryExpression gt(LiteralExpression literal);
        public abstract QueryExpression gte(LiteralExpression literal);
        public abstract QueryExpression lt(LiteralExpression literal);
        public abstract QueryExpression lte(LiteralExpression literal);

        public static QueryExpression create(TerminalExpression expression) {

            if (expression instanceof NamedFieldExpression) {
                return new SimpleQueryExpression((NamedFieldExpression) expression);
            }
            else if (expression instanceof NestedNamedFieldExpression) {
                return new NestedQueryExpression((NestedNamedFieldExpression) expression);
            }
            else {
                throw new DrillRuntimeException(format("Unsupported expression: [%s]", expression));
            }
        }
    }

    public static class CompoundQueryExpression extends QueryExpression {

        private BoolQueryBuilder builder = boolQuery();

        public static CompoundQueryExpression or(QueryExpression left, QueryExpression right) {
            CompoundQueryExpression bqe = new CompoundQueryExpression();
            bqe.builder.should(left.builder());
            bqe.builder.should(right.builder());
            return bqe;
        }

        public static CompoundQueryExpression and(QueryExpression left, QueryExpression right) {
            CompoundQueryExpression bqe = new CompoundQueryExpression();
            bqe.builder.must(left.builder());
            bqe.builder.must(right.builder());
            return bqe;
        }

        @Override
        public QueryBuilder builder() {
            return builder;
        }

        @Override
        public QueryExpression equals(LiteralExpression literal) {
            throw new DrillRuntimeException("Operator ['='] cannot be applied to a compound expression");
        }

        @Override
        public QueryExpression not(LiteralExpression literal) {
            throw new DrillRuntimeException("Operator ['not'] cannot be applied to a compound expression");
        }

        @Override
        public QueryExpression gt(LiteralExpression literal) {
            throw new DrillRuntimeException("Operator ['>'] cannot be applied to a compound expression");
        }

        @Override
        public QueryExpression gte(LiteralExpression literal) {
            throw new DrillRuntimeException("Operator ['>='] cannot be applied to a compound expression");
        }

        @Override
        public QueryExpression lt(LiteralExpression literal) {
            throw new DrillRuntimeException("Operator ['<'] cannot be applied to a compound expression");
        }

        @Override
        public QueryExpression lte(LiteralExpression literal) {
            throw new DrillRuntimeException("Operator ['<='] cannot be applied to a compound expression");
        }
    }

    public static class SimpleQueryExpression extends QueryExpression {

        private final NamedFieldExpression rel;
        private QueryBuilder builder;

        public SimpleQueryExpression(NamedFieldExpression rel) {
            this.rel = rel;
        }

        @Override
        public QueryBuilder builder() {
            return builder;
        }

        @Override
        public QueryExpression equals(LiteralExpression literal) {
            builder = termQuery(rel.field.getName(), literal.value());
            return this;
        }

        @Override
        public QueryExpression not(LiteralExpression literal) {
            builder = notQuery(termQuery(rel.field.getName(), literal.value()));
            return this;
        }

        @Override
        public QueryExpression gt(LiteralExpression literal) {
            builder = rangeQuery(rel.field.getName()).gt(literal.value());
            return this;
        }

        @Override
        public QueryExpression gte(LiteralExpression literal) {
            builder = rangeQuery(rel.field.getName()).gte(literal.value());
            return this;
        }

        @Override
        public QueryExpression lt(LiteralExpression literal) {
            builder = rangeQuery(rel.field.getName()).lt(literal.value());
            return this;
        }

        @Override
        public QueryExpression lte(LiteralExpression literal) {
            builder = rangeQuery(rel.field.getName()).lte(literal.value());
            return this;
        }
    }

    public static class NestedQueryExpression extends QueryExpression {

        private final NestedNamedFieldExpression rel;
        private NestedQueryBuilder builder;

        public NestedQueryExpression(NestedNamedFieldExpression rel) {
            this.rel = rel;
        }

        @Override
        public QueryBuilder builder() {
            return builder;
        }

        @Override
        public QueryExpression equals(LiteralExpression literal) {
            builder = nestedQuery(rel.enclosingPath, termQuery(rel.nestedPath, literal.value()));
            return this;
        }

        @Override
        public QueryExpression not(LiteralExpression literal) {
            builder = nestedQuery(rel.enclosingPath, notQuery(termQuery(rel.nestedPath, literal.value())));
            return this;
        }

        @Override
        public QueryExpression gt(LiteralExpression literal) {
            throw new UnsupportedOperationException("Unimplemented");
        }
        @Override
        public QueryExpression gte(LiteralExpression literal) {
            throw new UnsupportedOperationException("Unimplemented");
        }
        @Override
        public QueryExpression lt(LiteralExpression literal) {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public QueryExpression lte(LiteralExpression literal) {
            throw new UnsupportedOperationException("Unimplemented");
        }
    }

    /** Empty interface; exists only to define type hierarchy */
    public interface TerminalExpression extends Expression { }

    public static final class NestedNamedFieldExpression implements TerminalExpression {

        public final RelDataTypeField           field;
        public final RexLiteral                 literal;
        public final NestedNamedFieldExpression parent;

        public String enclosingPath;
        public String nestedPath;

        public NestedNamedFieldExpression(RelDataTypeField field, RexLiteral literal) {
            this(field, null, literal);
        }

        public NestedNamedFieldExpression(NestedNamedFieldExpression parent, RexLiteral literal) {
            this(null, parent, literal);
        }

        private NestedNamedFieldExpression(RelDataTypeField field, NestedNamedFieldExpression parent,
                                           RexLiteral literal) {
            this.field   = field;
            this.parent  = parent;
            this.literal = literal;

            nestedPath = path(field, parent, literal);
            int i = nestedPath.lastIndexOf(".");
            if (i > 0) {
                enclosingPath = nestedPath.substring(0, i);
            }
        }

        private String path(RelDataTypeField field, NestedNamedFieldExpression parent, RexLiteral literal) {

            StringBuilder sb = new StringBuilder();
            if (parent != null && parent.nestedPath != null) {
                sb.append(parent.nestedPath);
            }
            if (field != null) {
                if (sb.length() > 0) {
                    sb.append(".");
                }
                sb.append(field.getName());
            }
            if (literal != null) {
                if (sb.length() > 0) {
                    sb.append(".");
                }
                sb.append(RexLiteral.stringValue(literal));
            }
            return sb.toString();
        }
    }

    public static final class NamedFieldExpression implements TerminalExpression {

        public final RelDataTypeField field;

        public NamedFieldExpression(RelDataTypeField field) {
            this.field = field;
        }
    }

    public static final class CastExpression implements TerminalExpression {

        public final MajorType         target;
        public final LiteralExpression argument;

        public CastExpression(MajorType target, LiteralExpression argument) {
            this.target   = target;
            this.argument = argument;
        }
    }

    public static final class LiteralExpression implements TerminalExpression {

        public final RexLiteral literal;

        public LiteralExpression(RexLiteral literal) {
            this.literal = literal;
        }

        public <T> T value() {

            if (isIntegral()) {
                return (T) (Long) longValue();
            }
            else if (isFloatingPoint()) {
                return (T) (Double) doubleValue();
            }
            else if (isBoolean()) {
                return (T) (Boolean) booleanValue();
            }
            else if (isString()) {
                return (T) RexLiteral.stringValue(literal);
            }
            else {
                return (T) rawValue();
            }
        }

        public boolean isIntegral() {

            // XXX - There has to be a less error prone way of doing this.

            if (RexLiteral.valueMatchesType(literal.getValue(), SqlTypeName.INTEGER, false)) {
                if (literal.getType().getSqlTypeName() == SqlTypeName.DECIMAL ||
                    literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE ||
                    literal.getType().getSqlTypeName() == SqlTypeName.FLOAT) {
                    return false;
                }
                return true;
            }
            return false;
        }

        public boolean isFloatingPoint() {
            return (RexLiteral.valueMatchesType(literal.getValue(), SqlTypeName.DOUBLE, false));
        }

        public boolean isBoolean() {
            return (RexLiteral.valueMatchesType(literal.getValue(), SqlTypeName.BOOLEAN, false));
        }

        public boolean isString() {
            return (RexLiteral.valueMatchesType(literal.getValue(), SqlTypeName.VARCHAR, false));
        }

        public long longValue() {
            return ((Number) literal.getValue()).longValue();
        }

        public double doubleValue() {
            return ((Number) literal.getValue()).doubleValue();
        }

        public boolean booleanValue() {
            return RexLiteral.booleanValue(literal);
        }

        public String stringValue() {
            return RexLiteral.stringValue(literal);
        }

        public Object rawValue() {
            return literal.getValue();
        }
    }

    public static String queryAsJson(QueryBuilder query) throws IOException {
        XContentBuilder x = XContentFactory.jsonBuilder();
        x.prettyPrint().lfAtEnd();
        query.toXContent(x, ToXContent.EMPTY_PARAMS);
        return x.string();
    }
}
