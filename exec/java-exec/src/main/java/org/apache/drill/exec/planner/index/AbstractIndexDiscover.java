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

import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.StoragePluginRegistry;

/*
 * AbstractIndexDiscover is the layer to read index configurations of tables on storage plugins,
 * then based on the properties it collected, get the StoragePlugin from StoragePluginRegistry,
 * together with indexes information, build an IndexCollection
 */
public abstract class AbstractIndexDiscover implements IndexDiscover {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractIndexDiscover.class);

    //from ElasticSearchConstant
    static final String ES_CONFIG_KEY_CLUSTER = "elasticsearch.config.cluster";

    public abstract AbstractGroupScan getOriginalScan();
    /**
     *Implement in derived classes that have the details of storage and
     */
    public abstract StoragePluginRegistry getStorageRegistry();

    /**
     * Abstract function getDrillTable will be implemented the IndexDiscover within storage plugin(e.g. HBase, MaprDB)
     * since the implementations of AbstractStoragePlugin, IndexDescriptor and DrillTable in that storage plugin may have
     * the implement details.
     * @param idxDesc

     * @return
     */
    public abstract DrillTable getDrillTable(IndexDescriptor idxDesc);
}
