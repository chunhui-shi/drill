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

package org.apache.drill.exec.physical.impl.join;


import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.resolver.TypeCastRules;

import java.util.LinkedList;
import java.util.List;

public class JoinUtils {

  public enum JoinCategory {
    EQUALITY,  // equality join
    INEQUALITY,  // inequality join: <>, <, >
    CARTESIAN   // no join condition
  }
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinUtils.class);

  // Check the comparator is supported in join condition. Note that a similar check is also
  // done in JoinPrel; however we have to repeat it here because a physical plan
  // may be submitted directly to Drill.
  public static Comparator checkAndReturnSupportedJoinComparator(JoinCondition condition) {
    switch(condition.getRelationship().toUpperCase()) {
      case "EQUALS":
      case "==": /* older json plans still have '==' */
        return Comparator.EQUALS;
      case "IS_NOT_DISTINCT_FROM":
        return Comparator.IS_NOT_DISTINCT_FROM;
    }
    throw UserException.unsupportedError()
        .message("Invalid comparator supplied to this join: ", condition.getRelationship())
        .build(logger);
  }


  /**
   * Checks if implicit cast is allowed between the two input types of the join condition. Currently we allow
   * implicit casts in join condition only between numeric types and varchar/varbinary types.
   * @param input1
   * @param input2
   * @return true if implicit cast is allowed false otherwise
   */
  private static boolean allowImplicitCast(TypeProtos.MinorType input1, TypeProtos.MinorType input2) {
    // allow implicit cast if both the input types are numeric
    if (TypeCastRules.isNumericType(input1) && TypeCastRules.isNumericType(input2)) {
      return true;
    }

    // allow implicit cast if input types are date/ timestamp
    if ((input1 == TypeProtos.MinorType.DATE || input1 == TypeProtos.MinorType.TIMESTAMP) &&
        (input2 == TypeProtos.MinorType.DATE || input2 == TypeProtos.MinorType.TIMESTAMP)) {
      return true;
    }

    // allow implicit cast if both the input types are varbinary/ varchar
    if ((input1 == TypeProtos.MinorType.VARCHAR || input1 == TypeProtos.MinorType.VARBINARY) &&
        (input2 == TypeProtos.MinorType.VARCHAR || input2 == TypeProtos.MinorType.VARBINARY)) {
      return true;
    }

    return false;
  }

  /**
   * Utility method used by joins to add implicit casts on one of the sides of the join condition in case the two
   * expressions have different types.
   * @param leftExpressions array of expressions from left input into the join
   * @param leftBatch left input record batch
   * @param rightExpressions array of expressions from right input into the join
   * @param rightBatch right input record batch
   * @param context fragment context
   */
  public static void addLeastRestrictiveCasts(LogicalExpression[] leftExpressions, VectorAccessible leftBatch,
                                              LogicalExpression[] rightExpressions, VectorAccessible rightBatch,
                                              FragmentContext context) {
    assert rightExpressions.length == leftExpressions.length;

    for (int i = 0; i < rightExpressions.length; i++) {
      LogicalExpression rightExpression = rightExpressions[i];
      LogicalExpression leftExpression = leftExpressions[i];
      TypeProtos.MinorType rightType = rightExpression.getMajorType().getMinorType();
      TypeProtos.MinorType leftType = leftExpression.getMajorType().getMinorType();

      if (rightType == TypeProtos.MinorType.UNION || leftType == TypeProtos.MinorType.UNION) {
        continue;
      }
      if (rightType != leftType) {

        // currently we only support implicit casts if the input types are numeric or varchar/varbinary
        if (!allowImplicitCast(rightType, leftType)) {
          throw new DrillRuntimeException(String.format("Join only supports implicit casts between " +
              "1. Numeric data\n 2. Varchar, Varbinary data 3. Date, Timestamp data " +
              "Left type: %s, Right type: %s. Add explicit casts to avoid this error", leftType, rightType));
        }

        // We need to add a cast to one of the expressions
        List<TypeProtos.MinorType> types = new LinkedList<>();
        types.add(rightType);
        types.add(leftType);
        TypeProtos.MinorType result = TypeCastRules.getLeastRestrictiveType(types);
        ErrorCollector errorCollector = new ErrorCollectorImpl();

        if (result == null) {
          throw new DrillRuntimeException(String.format("Join conditions cannot be compared failing left " +
                  "expression:" + " %s failing right expression: %s", leftExpression.getMajorType().toString(),
              rightExpression.getMajorType().toString()));
        } else if (result != rightType) {
          // Add a cast expression on top of the right expression
          LogicalExpression castExpr = ExpressionTreeMaterializer.addCastExpression(rightExpression, leftExpression.getMajorType(), context.getFunctionRegistry(), errorCollector);
          // Store the newly casted expression
          rightExpressions[i] =
              ExpressionTreeMaterializer.materialize(castExpr, rightBatch, errorCollector,
                  context.getFunctionRegistry());
        } else if (result != leftType) {
          // Add a cast expression on top of the left expression
          LogicalExpression castExpr = ExpressionTreeMaterializer.addCastExpression(leftExpression, rightExpression.getMajorType(), context.getFunctionRegistry(), errorCollector);
          // store the newly casted expression
          leftExpressions[i] =
              ExpressionTreeMaterializer.materialize(castExpr, leftBatch, errorCollector,
                  context.getFunctionRegistry());
        }
      }
    }
  }



}
