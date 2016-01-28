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
package org.apache.drill.exec.planner.index;

import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_DEFAULT_BATCH_SIZE;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_BATCH_SIZE;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_CLUSTER;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_HOSTS;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePluginConfig;
import org.apache.drill.exec.store.elasticsearch.rules.CountWrapper;

public class HBaseESIndexDescriptor extends AbstractIndexDescriptor {
  private static final double DEFAULT_SELECTIVITY = 0.01;

  private final ScanPrel scanPrel;
  private final ElasticsearchStoragePluginConfig esConfig;
  private final String indexName;

  public HBaseESIndexDescriptor(PlannerSettings settings, ScanPrel scanPrel) {
    super(((HBaseGroupScan)scanPrel.getGroupScan()).getSecondaryIndexColumns());
    this.scanPrel = scanPrel;
    this.indexName = getIndexName();
    this.esConfig = getConfig();
  }

  public String getIndexName() {
    return "tempIndex";
  }

  public ElasticsearchStoragePluginConfig getConfig() {
    Map<String, String> map = new HashMap<>();
    map.put(ES_CONFIG_KEY_HOSTS, "10.10.10.191");
    map.put(ES_CONFIG_KEY_CLUSTER, "aman-vmcluster");
    map.put(ES_CONFIG_KEY_BATCH_SIZE, ES_CONFIG_DEFAULT_BATCH_SIZE);

    ElasticsearchStoragePluginConfig config = new ElasticsearchStoragePluginConfig(map);
    config.setEnabled(true);
    return config;
  }

  @Override
  public double getRows(RexNode indexCondition) {
    // TODO: Use the Elasticsearch COUNT API to compute the selectivity of the predicate
    // return row count based on default selectivity for now;
    HBaseGroupScan hbscan = (HBaseGroupScan)scanPrel.getGroupScan();
    //     return DEFAULT_SELECTIVITY * hbscan.getScanStats().getRecordCount();

    // Call the wrapper for creating a COUNT query
    return CountWrapper.getCount(scanPrel, indexCondition, indexName, esConfig);
  }

  @Override
  public AbstractGroupScan getIndexGroupScan() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IndexType getIndexType() {
    return IndexDescriptor.IndexType.EXTERNAL_SECONDARY_INDEX;
  }

}
