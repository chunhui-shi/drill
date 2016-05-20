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

import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePlugin;

import java.util.Set;

public class HBaseIndexCollection extends AbstractIndexCollection {


    private final StoragePlugin storage;
    private final ScanPrel scan;

    private String name;

    public HBaseIndexCollection(ScanPrel prel,
                                Set<HBaseIndexDescriptor> indexes) {

        this.storage = indexes.iterator().next().getDrillTable().getPlugin();
        this.scan = prel;
        for (IndexDescriptor index : indexes) {
            super.addIndex(index);
        }
    }

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
    public double getRows(RexNode indexCondition, IndexDescriptor indexDesc) {
        return indexDesc.getRows(indexCondition);
    }

    @Override
    public IndexGroupScan getGroupScan() {
        return null;
    }

    @Override
    public IndexCollectionType getIndexCollectionType() {
        return IndexCollection.IndexCollectionType.EXTERNAL_SECONDARY_INDEX_COLLECTION;
    }


}
