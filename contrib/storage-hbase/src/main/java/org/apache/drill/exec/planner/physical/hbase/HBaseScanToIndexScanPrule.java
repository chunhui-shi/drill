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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.FindPartitionConditions;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.logical.partition.RewriteCombineBinaryOperators;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.SubsetTransformer;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;
import org.apache.drill.exec.planner.index.IndexCollection;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.Lists;

// import org.apache.drill.exec.store.elasticsearch.ElasticsearchGroupScan;
// import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePlugin;

public abstract class HBaseScanToIndexScanPrule extends StoragePluginOptimizerRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseScanToIndexScanPrule.class);

  /**
   * Selectivity threshold below which an index scan plan would be generated.
   */
  static final double INDEX_SELECTIVITY_THRESHOLD = 0.1;

  /**
   * Return the index collection relevant for the underlying data source
   * @param settings
   * @param scan
   */
  public abstract IndexCollection getIndexCollection(PlannerSettings settings, ScanPrel scan);

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
      public IndexCollection getIndexCollection(PlannerSettings settings, ScanPrel scan) {
        HBaseGroupScan groupScan = (HBaseGroupScan)scan.getGroupScan();
        return groupScan.getSecondaryIndexCollection();
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(2);
        GroupScan groupScan = scan.getGroupScan();
        if (groupScan instanceof HBaseGroupScan) {
          HBaseGroupScan hbscan = ((HBaseGroupScan)groupScan);
          return hbscan.supportsSecondaryIndex();
        }
        return false;
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
      public IndexCollection getIndexCollection(PlannerSettings settings, ScanPrel scan) {
        HBaseGroupScan groupScan = (HBaseGroupScan)scan.getGroupScan();
        return groupScan.getSecondaryIndexCollection();
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(1);
        GroupScan groupScan = scan.getGroupScan();
        if (groupScan instanceof HBaseGroupScan) {
          HBaseGroupScan hbscan = ((HBaseGroupScan)groupScan);
          return hbscan.supportsSecondaryIndex();
        }
        return false;
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
   */

  protected void doOnMatch(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, ScanPrel scan) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    final IndexCollection indexCollection = getIndexCollection(settings, scan);
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

    if (indexCollection.supportsIndexSelection()) {
      processWithoutIndexSelection(call, settings, condition,
          indexCollection, builder, filter, scan);
    } else {
      processWithIndexSelection(call, settings, condition,
          indexCollection, builder, filter, scan);
    }
  }


  /**
   *
   */
  private void processWithoutIndexSelection(
      RelOptRuleCall call,
      PlannerSettings settings,
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder,
      FilterPrel filter,
      ScanPrel scan) {
    List<String> fieldNames = scan.getRowType().getFieldNames();
    //    BitSet indexColumnBitSet = new BitSet();
    BitSet columnBitSet = new BitSet();

    int relColIndex = 0; // index into the rowtype for the indexed columns
    boolean indexedCol = false;
    for (String field : fieldNames) {
      // final int indexColIndex = descriptor.getColumnOrdinal(field);
      // if (indexColIndex != -1) {
      //   indexColumnBitSet.set(indexColIndex);
      if (collection.isColumnIndexed(field)) {
        columnBitSet.set(relColIndex);
        indexedCol = true;
      }
      relColIndex++;
    }

    if (!indexedCol) {
      logger.debug("No index columns are projected from the scan..continue.");
      return;
    }

    // Use the same filter analyzer that is used for partitioning columns
    FindPartitionConditions c = new FindPartitionConditions(columnBitSet, builder);
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
      if (collection.supportsRowCountStats()) {
        double indexRows = collection.getRows(indexCondition);
        // get the selectivity of the predicates on the index columns
        double selectivity = indexRows/scan.getRows();

        if (selectivity < INDEX_SELECTIVITY_THRESHOLD &&
            indexRows < settings.getBroadcastThreshold()) {
          IndexPlanGenerator planGen = new IndexPlanGenerator(call, collection, indexCondition, remainderCondition, builder);
          planGen.go(filter, scan);
        }
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to build index access plan", e);
    }

  }

  // Generate an equivalent plan with HashJoin
  // The right child of hash join consists of the index group scan followed by filter containing the index condition.
  // Left child of hash join consists of the table scan followed by the same filter containing the index condition.
  private class IndexPlanGenerator extends SubsetTransformer<FilterPrel, InvalidRelException> {
    final private IndexCollection indexCollection;
    final private RexNode indexCondition;
    final private RexNode remainderCondition;
    final private RexBuilder builder;

    public IndexPlanGenerator(RelOptRuleCall call, IndexCollection indexCollection,
        RexNode indexCondition,
        RexNode remainderCondition,
        RexBuilder builder) {
      super(call);
      this.indexCollection = indexCollection;
      this.indexCondition = indexCondition;
      this.remainderCondition = remainderCondition;
      this.builder = builder;
    }

    @Override
    public RelNode convertChild(final FilterPrel filter, final RelNode scan) throws InvalidRelException {

      GroupScan indexGroupScan = indexCollection.getGroupScan();
      if (indexGroupScan != null) {
        ScanPrel indexScanPrel = new ScanPrel(scan.getCluster(),
            scan.getTraitSet(), indexGroupScan, scan.getRowType());

        // right (build) side of the hash join: broadcast the filter-indexscan subplan
        FilterPrel rightIndexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
            indexScanPrel, indexCondition);
        final DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
        RelTraitSet rightTraits = newTraitSet(distBroadcastRight).plus(Prel.DRILL_PHYSICAL);
        RelNode convertedRight = convert(rightIndexFilterPrel, rightTraits);

        // left (probe) side of the hash join
        FilterPrel leftIndexFilterPrel = new FilterPrel(scan.getCluster(), scan.getTraitSet(),
            scan, indexCondition);
        final RelTraitSet leftTraits = scan.getTraitSet().plus(Prel.DRILL_PHYSICAL);
        final RelNode convertedLeft = convert(leftIndexFilterPrel, leftTraits);

        // find the rowkey column on the left side of join
        // TODO: is there a shortcut way to do this ?
        List<String> leftFieldNames = convertedLeft.getRowType().getFieldNames();
        int idx = 0;
        int leftRowKeyIdx = -1;
        for (String field : leftFieldNames) {
          if (field.equalsIgnoreCase(DrillHBaseConstants.ROW_KEY)) {
            leftRowKeyIdx = idx;
            break;
          }
          idx++;
        }

        // TODO: get the rowkey expr from the right side of join

        List<RexNode> joinConjuncts = Lists.newArrayList();
        joinConjuncts.add(
            builder.makeCall(SqlStdOperatorTable.EQUALS,
                RexInputRef.of(leftRowKeyIdx, convertedLeft.getRowType()),
                RexInputRef.of(0,  convertedRight.getRowType())));

        RexNode joinCondition = RexUtil.composeConjunction(builder, joinConjuncts, false);

        HashJoinPrel hjPrel = new HashJoinPrel(filter.getCluster(), leftTraits, convertedLeft,
            convertedRight, joinCondition, JoinRelType.INNER);

        RelNode newRel = hjPrel;

        if (remainderCondition != null && !remainderCondition.isAlwaysTrue()) {
          // create a Filter corresponding to the remainder condition
          FilterPrel remainderFilterPrel = new FilterPrel(hjPrel.getCluster(), hjPrel.getTraitSet(),
              hjPrel, remainderCondition);
          newRel = remainderFilterPrel;
        }

        RelNode finalRel = convert(newRel, hjPrel.getTraitSet());

        return finalRel;
      }
      return null;
    }
  }

  private void processWithIndexSelection(
      RelOptRuleCall call,
      PlannerSettings settings,
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder,
      FilterPrel filter,
      ScanPrel scan) {

  }

  /**
   * For a particular table scan for table T1 and an index on that table, find out if it is a covering index
   * @return
   */
  private boolean isCoveringIndex(ScanPrel scan, IndexDescriptor index) {
    HBaseGroupScan groupScan = (HBaseGroupScan)scan.getGroupScan();
    List<SchemaPath> tableCols = groupScan.getColumns();
    return index.isCoveringIndex(tableCols);
  }

}

