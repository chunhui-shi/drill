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

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.planner.index.IndexStatistic;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePlugin;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePluginConfig;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWrapper extends IndexStatistic {

  public CountWrapper(RelNode input, RexNode condition,  DrillTable table) {
    super(input, condition, table);
  }

  @Override
  public Double getRowCount() {
    long count = -1;
    try {
      PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(this.input, this.condition);
      //String query = PredicateAnalyzer.queryAsJson(analyzed.builder());
      StoragePluginConfig config = this.table.getStorageEngineConfig();
      ElasticsearchStoragePluginConfig esconfig = config instanceof ElasticsearchStoragePluginConfig? (ElasticsearchStoragePluginConfig)config:null;
      CountRequestBuilder cb = esconfig.getClient().prepareCount(((DrillElasticsearchTable)this.table).name()).setQuery(analyzed.builder());
      count = cb.get().getCount();

    } catch (ExpressionNotAnalyzableException e) {
      logger.warn("Encountered exception while getting COUNT: ", e);
    }

    return new Double(count);
  }

  @Override
  public RelDistribution getDistribution() {
    return null;
  }

}
