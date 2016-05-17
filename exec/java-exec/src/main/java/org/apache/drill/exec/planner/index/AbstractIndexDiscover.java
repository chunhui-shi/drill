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

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

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
     * With the provided information, build a list of IndexDescriptor
     * @param tables
     * @return
     */
    public abstract Set<IndexDescriptor> getIndexDescriptors(Set<DrillTable> tables);

    private DrillTable getDrillTable(Map<String, String> param, AbstractStoragePlugin idxStorage, String idxStorageName) {

        String indexInfo = param.get("table");
        String[] tableInfo = indexInfo.split(":");
        String[] pathInfo = new String[2];
        if(tableInfo.length == 2) {
            pathInfo = tableInfo[1].split("/");
        }
        String cluster = tableInfo[0];
        String schema = pathInfo[0];
        String idxTableName = pathInfo[1];

        if(idxStorage == null) {
            StoragePluginRegistry registry = getStorageRegistry();

            for (Map.Entry<String, StoragePlugin> entry : registry) {
                if (!(entry.getValue() instanceof AbstractStoragePlugin)) {
                    continue;
                }

                AbstractStoragePlugin tmpStorage = (AbstractStoragePlugin) entry.getValue();
                //check elasticsearch config parameters, if they are matching, use this storagePlugIn
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
        return ((IndexDiscoverable) schemaFactory).findTable(tableNames);

    }

    public Set<IndexDescriptor> getIndexCollection(List<Map<String,String>> indexParams) {
        //XXX we now consider only one storage plugin for indexes

        AbstractStoragePlugin idxStorage = null;

        Set<DrillTable> tablesAsIndexes = new HashSet<>();
        DrillTable idxTable = null;
        for(Map<String, String> param: indexParams) {

            // get the storage and table name(schema+table) of the index
            idxTable = null;
            String idxStorageName = null;
            if(idxStorage == null) {
                idxTable = getDrillTable(param, null, null);
                idxStorage = (AbstractStoragePlugin)idxTable.getPlugin();
                idxStorageName = idxTable.getStorageEngineName();
            }
            else {
                idxTable = getDrillTable(param, idxStorage, idxStorageName);
            }
            if (idxTable == null) {
                logger.error("index table is null for [{}]!", param.toString());
                return null;
            }
            tablesAsIndexes.add(idxTable);
        }

        //with table objects,IndexDescs (from mfs), construct IndexCollection,

        return getIndexDescriptors(tablesAsIndexes);

    }



}
