/**
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

package org.apache.drill.exec.planner.physical.hbase;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.FileSystemPartitionDescriptor;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.FindPartitionConditions;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.logical.partition.RewriteCombineBinaryOperators;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrule;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hbase.HBaseFilterBuilder;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;
import org.apache.drill.exec.planner.index.HBaseSecondaryIndexDescriptor;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// import org.apache.drill.exec.store.elasticsearch.ElasticsearchGroupScan;
// import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePlugin;

public abstract class HBaseScanToIndexScanPrule extends StoragePluginOptimizerRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseScanToIndexScanPrule.class);

  /**
   * Selectivity threshold below which an index scan plan would be generated.
   */
  static final double INDEX_SELECTIVITY_THRESHOLD = 0.1;

  public abstract IndexDescriptor getIndexDescriptor(PlannerSettings settings, ScanPrel scan);

  final OptimizerRulesContext optimizerContext;

  public HBaseScanToIndexScanPrule(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.optimizerContext = optimizerContext;
  }

  public static final RelOptRule getFilterOnProject(OptimizerRulesContext optimizerRulesContext) {
    return new HBaseScanToIndexScanPrule(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
        "HBaseScanToIndexScanPrule:Filter_On_Project",
        optimizerRulesContext) {

      @Override
      public IndexDescriptor getIndexDescriptor(PlannerSettings settings, ScanPrel scan) {
        return new HBaseSecondaryIndexDescriptor(settings, scan);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(2);
        GroupScan groupScan = scan.getGroupScan();
        return groupScan instanceof HBaseGroupScan && ((HBaseGroupScan)groupScan).supportsSecondaryIndex();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final FilterPrel filter = (FilterPrel) call.rel(0);
        final ProjectPrel project = (ProjectPrel) call.rel(1);
        final ScanPrel scan = (ScanPrel) call.rel(2);
        doOnMatch(call, filter, project, scan);
      }
    };
  }

  public static final RelOptRule getFilterOnScan(OptimizerRulesContext optimizerRulesContext) {
    return new HBaseScanToIndexScanPrule(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
        "HBaseScanToIndexScanPrule:Filter_On_Scan", optimizerRulesContext) {

      @Override
      public IndexDescriptor getIndexDescriptor(PlannerSettings settings, ScanPrel scan) {
        return new HBaseSecondaryIndexDescriptor(settings, scan);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(1);
        GroupScan groupScan = scan.getGroupScan();
        return groupScan instanceof HBaseGroupScan && ((HBaseGroupScan)groupScan).supportsSecondaryIndex();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final FilterPrel filter = (FilterPrel) call.rel(0);
        final ScanPrel scan = (ScanPrel) call.rel(1);
        doOnMatch(call, filter, null, scan);
      }
    };
  }


  /*
   * Original Plan:
   *               Filter
   *                 |
   *            HBaseGroupScan
   *
   */

  /*
   * New Plan:
   *            Filter (Original filter minus filters on index cols)
   *               |
   *            HashJoin
   *          /          \
   *  HBaseGroupScan   Filter (with index cols only)
   *                      |
   *                  ESGroupScan
   *
   * This plan will be further optimized by the ElasticSearch storage optimizer rule
   * where the Filter with index columns will be pushed into the ESGroupScan
   *
   * A better Plan:
   *
   *            Filter (original filter minus filters on index cols)
   *               |
   *            RowkeyJoin
   *          /      \
   *         /        HBaseGroupScan
   *     Filter
   * (with index cols only)
   *       |
   *  ESGroupScan
   *
   *
   */

  protected void doOnMatch(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, ScanPrel scan) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    final IndexDescriptor descriptor = getIndexDescriptor(settings, scan);
    //    final BufferAllocator allocator = optimizerContext.getAllocator();
    RexBuilder builder = filter.getCluster().getRexBuilder();

    RexNode condition = null;
    if (project == null) {
      condition = filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(filter.getCondition(), project);
    }

    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, builder);
    condition = condition.accept(visitor);

    Map<Integer, String> fieldNameMap = Maps.newHashMap();
    List<String> fieldNames = scan.getRowType().getFieldNames();
    BitSet indexColumnBitSet = new BitSet();
    BitSet columnBitSet = new BitSet();

    int relColIndex = 0; // index into the rowtype for the indexed columns
    for (String field : fieldNames) {
      final Integer indexColIndex = descriptor.getIdIfValid(field);
      if (indexColIndex != null) {
        fieldNameMap.put(indexColIndex, field);
        indexColumnBitSet.set(indexColIndex);
        columnBitSet.set(relColIndex);
      }
      relColIndex++;
    }

    if (indexColumnBitSet.isEmpty()) {
      logger.debug("No index columns are projected from the scan..continue.");
      return;
    }

    FindPartitionConditions c = new FindPartitionConditions(indexColumnBitSet, builder);
    c.analyze(condition);
    RexNode indexCondition = c.getFinalCondition();

    if (indexCondition == null) {
      logger.debug("No conditions were found eligible for applying index lookup.");
      return;
    }

    List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
    List<RexNode> indexConjuncts = RelOptUtil.conjunctions(indexCondition);
    conjuncts.removeAll(indexConjuncts);
    RexNode remainderCondition = RexUtil.composeConjunction(builder, conjuncts, false);

    RewriteCombineBinaryOperators reverseVisitor =
        new RewriteCombineBinaryOperators(true, builder);

    condition = condition.accept(reverseVisitor);
    indexCondition = indexCondition.accept(reverseVisitor);

    try {
      if (descriptor.supportsRowCountStats()) {
        double indexRows = descriptor.getRows(indexCondition);
        // get the selectivity of the predicates on the index columns
        double selectivity = indexRows/scan.getRows();

        if (selectivity < INDEX_SELECTIVITY_THRESHOLD &&
            indexRows < settings.getBroadcastThreshold()) {
          AbstractGroupScan indexGroupScan = descriptor.getIndexGroupScan();
          ScanPrel esScanPrel = new ScanPrel(scan.getCluster(),
              scan.getTraitSet(), indexGroupScan, scan.getRowType());

          FilterPrel indexFilterPrel = new FilterPrel(scan.getCluster(), scan.getTraitSet(),
              esScanPrel, indexCondition);

          // create a HashJoin broadcast inner plan.
          final DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
          final RelTraitSet traitsRight = null;
          final RelNode left = scan;
          final RelNode right = indexFilterPrel;
          final RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL);
          final RelNode convertedLeft = convert(left, traitsLeft);
          final RelNode convertedRight = convert(right, traitsRight);
          final int rowkeyIdx = 0; /* TODO: need to figure out how to get rowkey index since the left
                                  child need not be the HBaseGroupScan but some other node such
                                  as Filter, Project etc.*/

          List<RexNode> joinConjuncts = Lists.newArrayList();
          joinConjuncts.add(
              builder.makeCall(SqlStdOperatorTable.EQUALS,
                  RexInputRef.of(rowkeyIdx, convertedLeft.getRowType()),
                  RexInputRef.of(0,  convertedRight.getRowType())));

          RexNode joinCondition = RexUtil.composeConjunction(builder, joinConjuncts, false);

          HashJoinPrel hj = new HashJoinPrel(scan.getCluster(), traitsLeft, convertedLeft, convertedRight, joinCondition,
              JoinRelType.INNER);

          // create a Filter corresponding to the remainder condition
          FilterPrel remainderFilterPrel = new FilterPrel(hj.getCluster(), hj.getTraitSet(),
              hj, remainderCondition);

          call.transformTo(remainderFilterPrel);

        }
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to build index access plan", e);
    }

  }

}
