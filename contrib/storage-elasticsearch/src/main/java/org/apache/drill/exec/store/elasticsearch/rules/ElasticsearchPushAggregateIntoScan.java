/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.drill.exec.store.elasticsearch.rules;

import org.apache.calcite.plan.RelOptRuleCall;

import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.AggPrelBase;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites SQL aggregate functions in an attempt to have elasticsearch execute
 * them directly. Elasticsearch directly supports a large number of aggregate operators,
 * many of which have direct equivalents in SQL.
 *
 * @see "https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html"
 */
public class ElasticsearchPushAggregateIntoScan extends StoragePluginOptimizerRule {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchPushAggregateIntoScan.class);

    public static final StoragePluginOptimizerRule INSTANCE = new ElasticsearchPushAggregateIntoScan();

    public ElasticsearchPushAggregateIntoScan() {
        super(RelOptHelper.some(AggPrelBase.class, RelOptHelper.any(ScanPrel.class)), "ElasticsearchPushAggregateIntoScan");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // TBD
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // TBD
        logger.info("Matched optimizer rule: [{}]", call);
    }
}
