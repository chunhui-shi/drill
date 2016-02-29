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

import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchGroupScan;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchScanSpec;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePlugin;
import org.apache.drill.exec.store.elasticsearch.rules.CountWrapper;

import com.google.common.collect.ImmutableSet;

public class HBaseESIndexCollection extends AbstractIndexCollection {
  private static final double DEFAULT_SELECTIVITY = 0.01;

  private final ElasticsearchStoragePlugin esPlugin;
  private final ElasticsearchScanSpec esScanSpec;
  private final String indexName;
  private final ScanPrel tableScanPrel;

  public HBaseESIndexCollection(ElasticsearchStoragePlugin esPlugin,
      ElasticsearchScanSpec esScanSpec,
      List<IndexDescriptor> indexes,
      ScanPrel tableScanPrel) {
    for (IndexDescriptor index : indexes) {
      super.addIndex(index);
    }

    this.esPlugin = esPlugin;
    this.esScanSpec = esScanSpec;
    this.indexName = getIndexName();
    this.tableScanPrel = tableScanPrel;
  }

  public String getIndexName() {
    return "tempIndex";
  }

  /*
  public ElasticsearchStoragePluginConfig getConfig() {
    Map<String, String> map = new HashMap<>();
    map.put(ES_CONFIG_KEY_HOSTS, "10.10.101.41");
    map.put(ES_CONFIG_KEY_CLUSTER, "aman-vmcluster");
    map.put(ES_CONFIG_KEY_BATCH_SIZE, ES_CONFIG_DEFAULT_BATCH_SIZE);

    ElasticsearchStoragePluginConfig config = new ElasticsearchStoragePluginConfig(map);
    config.setEnabled(true);
    return config;
  }
   */

  @Override
  public boolean supportsIndexSelection() {
    return true;
  }

  @Override
  public boolean supportsRowCountStats() {
    return true;
  }

  @Override
  public boolean supportsFullTextSearch() {
    return true;
  }

  @Override
  public double getRows(RexNode indexCondition) {
    // Call the wrapper for creating a COUNT query
    return CountWrapper.getCount(tableScanPrel, indexCondition, indexName, esPlugin.getConfig());
  }

  @Override
  public GroupScan getGroupScan() {
    return new ElasticsearchGroupScan(tableScanPrel.getGroupScan().getUserName(),
        this.esPlugin, this.esScanSpec, null /* null implies all columns */);
  }

  @Override
  public IndexCollectionType getIndexCollectionType() {
    return IndexCollection.IndexCollectionType.EXTERNAL_SECONDARY_INDEX_COLLECTION;
  }

}
