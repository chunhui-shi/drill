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


import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.tables.IndexDesc;
import com.mapr.fs.tables.IndexFieldDesc;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;

public class HBaseIndexDiscover extends AbstractIndexDiscover implements IndexDiscover {

    private HBaseGroupScan scan;
    private ScanPrel scanPrel;

    public HBaseIndexDiscover(GroupScan inScan, ScanPrel prel) {
        if (inScan instanceof HBaseGroupScan) {
            this.scan = (HBaseGroupScan) inScan;
            this.scanPrel = prel;
        }
    }

    @Override
    public AbstractGroupScan getOriginalScan() {
        return scan;
    }

    @Override
    public StoragePluginRegistry getStorageRegistry() {
        return scan.getStoragePlugin().getContext().getStorage();
    }

    @Override
    public IndexCollection getTableIndex(String tableName) {
        //return getTableIndexFromCommandLine(tableName);
        return getTableIndexFromMFS(tableName);
    }

    /**
     *
     * @param tableName
     * @return
     */
    private IndexCollection getTableIndexFromMFS(String tableName) {
        MapRFileSystem mfs = getMaprFS();
        try {
            Set<HBaseIndexDescriptor> idxSet = new HashSet<>();
            Collection<IndexDesc> indexes = mfs.getTableIndexes(new Path(tableName));
            for (IndexDesc idx : indexes) {
                HBaseIndexDescriptor hbaseIdx = buildIndexDescriptor(tableName, idx);
                idxSet.add(hbaseIdx);
            }
            if(idxSet.size() == 0) {
                logger.error("No index found for table {}.", tableName);
                return null;
            }
            return new HBaseIndexCollection(scanPrel, idxSet);
        }
        catch(IOException ex) {
            logger.error("Could not get table index from File system. {}", ex.getMessage());
        }
        return null;
    }

    public DrillTable getDrillTable(IndexDescriptor idxDesc) {

        HBaseIndexDescriptor hbaseIdx = (HBaseIndexDescriptor)idxDesc;

        AbstractStoragePlugin idxStorage = null;
        String idxStorageName = "";

        String cluster = hbaseIdx.getClusterName();
        String[] indexInfo = idxDesc.getIndexName().split("/");
        if(indexInfo.length <= 1) {
            logger.error("ES index format should be like index/type but we got {}", idxDesc.getIndexName());
            return null;
        }
        String schema = indexInfo[0];
        String idxTableName = indexInfo[1];

        if(idxStorage == null) {
            StoragePluginRegistry registry = getStorageRegistry();

            for (Map.Entry<String, StoragePlugin> entry : registry) {
                if (!(entry.getValue() instanceof AbstractStoragePlugin)) {
                    continue;
                }

                AbstractStoragePlugin tmpStorage = (AbstractStoragePlugin) entry.getValue();
                //check config parameters, if they are matching, use this storagePlugIn
                if (tmpStorage.getConfig().isEnabled() && tmpStorage.getConfig().getValue(ES_CONFIG_KEY_CLUSTER) == cluster) {
                    idxStorage = tmpStorage;
                    idxStorageName = entry.getKey();
                    break;
                }
            }
        }

        //get table object for this index
        SchemaFactory schemaFactory = idxStorage.getSchemaFactory();
        if (! ( schemaFactory instanceof IndexDiscoverable ) ) {
            logger.warn("This Storage plugin does not support IndexDiscoverable interface: " + idxStorage.toString());
            return null;
        }

        List<String> tableNames = new ArrayList<>();
        tableNames.add(idxStorageName);
        tableNames.add(schema);
        tableNames.add(idxTableName);
        DrillTable foundTable = ((IndexDiscoverable) schemaFactory).findTable(tableNames);
        return foundTable;
    }

    private SchemaPath fieldName2SchemaPath(String fieldName) {
        return SchemaPath.getSimplePath(fieldName);
    }

    private List<SchemaPath> field2SchemaPath(Collection<IndexFieldDesc> descCollection) {
        List<SchemaPath> listSchema = new ArrayList<>();
        for (IndexFieldDesc field: descCollection) {
            listSchema.add(fieldName2SchemaPath(field.getFieldName()));
        }
        return listSchema;
    }

    private HBaseIndexDescriptor buildIndexDescriptor(String tableName, IndexDesc desc) {
        List<SchemaPath> rowkey = new ArrayList<>();
        rowkey.add(fieldName2SchemaPath("row_key"));
        IndexDescriptor.IndexType idxType = IndexDescriptor.IndexType.NATIVE_SECONDARY_INDEX;

        if (desc.getSystem().equalsIgnoreCase("maprdb")) {
            idxType = IndexDescriptor.IndexType.EXTERNAL_SECONDARY_INDEX;
        }

        HBaseIndexDescriptor idx = new HBaseIndexDescriptor (
                field2SchemaPath(desc.getIndexedFields()),
                field2SchemaPath(desc.getCoveredFields()),
                rowkey,
                desc.getIndexName(),
                tableName,
                idxType);
        idx.setClusterName(desc.getCluster());
        idx.setDrillTable(getDrillTable(idx));
        return idx;
    }

    private MapRFileSystem getMaprFS() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "maprfs:///");
        conf.set("fs.mapr.disable.namecache", "true");

        try {
            MapRFileSystem fs = new MapRFileSystem();
            URI fsuri = new URI(conf.get("fs.defaultFS"));
            fs.initialize(fsuri, conf);
            return fs;
        } catch (Exception var3) {
            return null;
        }

    }

}
