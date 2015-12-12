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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePluginConfig;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWrapper {

  private static final Logger logger = LoggerFactory.getLogger(CountWrapper.class);

  public static long getCount(RelNode input, RexNode condition, String indexName, ElasticsearchStoragePluginConfig config) {
    long count = -1;
    try {
      PredicateAnalyzer.QueryExpression analyzed = PredicateAnalyzer.analyze(input, condition);
      //String query = PredicateAnalyzer.queryAsJson(analyzed.builder());
      CountRequestBuilder cb = config.getClient().prepareCount(indexName).setQuery(analyzed.builder());
      count = cb.get().getCount();

    } catch (ExpressionNotAnalyzableException e) {
      logger.warn("Encountered exception: ", e);
    }

    return count;
  }

}
