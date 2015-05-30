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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchScanSpec;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchGroupScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Rewrites an SQL predicate into an elasticsearch filter that can be "pushed down" and
 * executed directly by elasticsearch.
 */
public abstract class ElasticsearchPushFilterIntoScan extends StoragePluginOptimizerRule {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchPushFilterIntoScan.class);

    private ElasticsearchPushFilterIntoScan(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new ElasticsearchPushFilterIntoScan(
            RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
            "ElasticsearchPushFilterIntoScan:Filter_On_Scan") {

        @Override
        public boolean matches(RelOptRuleCall call) {
            ScanPrel scan = call.rel(1);
            if (scan.getGroupScan() instanceof ElasticsearchGroupScan) {
                return super.matches(call);
            }
            return false;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {

            logger.debug("Matched optimizer rule: [{}]", call);

            FilterPrel filter = call.rel(0);
            ScanPrel scan     = call.rel(1);
            RexNode condition = filter.getCondition();

            ElasticsearchGroupScan group = (ElasticsearchGroupScan) scan.getGroupScan();
            if (group.isFilterPushedDown()) {
                logger.debug("Filter is already pushed down; skipping transform for rule: [{}]", call);
                return;
            }

            pushdown(call, filter, null, condition, scan, group);
        }
    };

    public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new ElasticsearchPushFilterIntoScan(
            RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
            "ElasticsearchPushFilterIntoScan:Filter_On_Project") {

        @Override
        public boolean matches(RelOptRuleCall call) {
            final ScanPrel scan = call.rel(2);
            if (scan.getGroupScan() instanceof ElasticsearchGroupScan) {
                return super.matches(call);
            }
            return false;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {

            logger.debug("Matched optimizer rule: [{}]", call);

            FilterPrel filter   = call.rel(0);
            ProjectPrel project = call.rel(1);
            ScanPrel scan       = call.rel(2);

            ElasticsearchGroupScan group = (ElasticsearchGroupScan) scan.getGroupScan();
            if (group.isFilterPushedDown()) {
                logger.debug("Filter is already pushed down; skipping transform for rule: [{}]", call);
                return;
            }

            // Convert the filter to one that references the child of the project.
            RexNode condition =  RelOptUtil.pushFilterPastProject(filter.getCondition(), project);

            pushdown(call, filter, project, condition, scan, group);
        }
    };

    /**
     * Transforms the call by providing a new scan with the filter pushed down.
     */
    protected void pushdown(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, RexNode condition,
                            ScanPrel scan, ElasticsearchGroupScan group) {

        String query = null;

        try {
            PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(scan, condition);
            query = PredicateAnalyzer.queryAsJson(analyzed.builder());
        }
        catch (ExpressionNotAnalyzableException | IOException e) {
            // Nothing could be transformed or we encountered a fatal error. The query will
            // still execute, but it will be slow b/c we can't push the predicate down to elasticsearch.
            logger.error("Failed to push filter into scan/project; falling back to manual filtering", e);
            return;
        }

        ElasticsearchScanSpec newScanSpec = new ElasticsearchScanSpec(group.getScanSpec().getSchema(),
                group.getScanSpec().getTable(), query);

        ElasticsearchGroupScan newGroupScan = new ElasticsearchGroupScan(group.getUserName(),
                group.getStoragePlugin(), newScanSpec, group.getColumns());

        newGroupScan.setFilterPushedDown(true);

        ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupScan, scan.getRowType());

        RelNode childRel;
        // Depending on whether is a project in the middle, assign either scan or copy of project to child rel.
        if (project == null) {
            childRel = newScanPrel;
        }
        else {
            childRel = project.copy(project.getTraitSet(), ImmutableList.of((RelNode) newScanPrel));
        }

        // XXX - Check here to see if entire filter was converted and optionally
        //       transform into something else? See HBasePushFilterIntoScan.onMatch()
        call.transformTo(childRel);
    }
}
