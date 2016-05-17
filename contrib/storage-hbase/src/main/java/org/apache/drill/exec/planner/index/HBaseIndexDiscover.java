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
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
        return getTableIndexFromCommandLine(tableName);
    }

    @Override
    public Set<IndexDescriptor> getIndexDescriptors(Set<DrillTable> tables) {

        return null;
    }

    //XXX temp solution before underlying maprfs system is available.
    /*
    idx  throttle  networkcompression  paused  table
    1    false     lz4                 false   elasticsearch:staffidx/stjson

    putsPending  type           bucketsPending  networkencryption
    0            Elasticsearch  0               false

    bytesPending  maxPendingTS  cluster  minPendingTS  uuid
    0             0             mapr520  0             d0ef78fc-ce6e-50ed-c1aa-0cd944265700

    synchronous  isUptodate  realTablePath
    false        true        /opt/.../staffidx/stjson/config.es
    */
    private IndexCollection getTableIndexFromCommandLine(String tableName) {
        //1st step is to get list of indexes (replicas in ES, for instance)
        //maprcli table replica list -path '/tmp/staff'

        List<String> listCommand = new ArrayList<>();
        listCommand.add("maprcli");
        listCommand.add("table");
        listCommand.add("replica");
        listCommand.add("list");
        listCommand.add("-path");
        listCommand.add("'" + tableName + "'");

        List<String> output = new ArrayList<>();

        try {
            ProcessBuilder pb = new ProcessBuilder(listCommand);
            Process process = pb.start();

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            String line = "";

            while ((line = stdInput.readLine()) != null) {
                output.add(line);
            }
        }
        catch(Exception e) {

        }
        if(output.size() <= 1) {
            //no replica, no index
            return null;
        }

        List<Map<String, String>> listReplicas = new ArrayList<>();
        String firstline = output.get(0);
        String[] columns = firstline.split("[ \t]+");

        for(int i=1; i<output.size(); ++i) {
            String[] fields = output.get(i).split("[ \t]+");
            if(fields.length >= columns.length) {
                Map<String, String> mapFields = new HashMap<>();
                for(int j=0; j<columns.length; ++j) {
                    mapFields.put(columns[j], fields[j]);
                }
                listReplicas.add(mapFields);
            }
        }

        Set<IndexDescriptor> setDesc = getIndexCollection(listReplicas);

        //indexes group by storageEngName, now assume there is only one storage
        Map<String, IndexCollection> mapCollection = new HashMap<>();

        for (IndexDescriptor desc : setDesc) {
            HBaseIndexDescriptor hbaseDesc = desc instanceof HBaseIndexDescriptor? (HBaseIndexDescriptor)desc:null;
            if (hbaseDesc == null) {
                continue;
            }
            AbstractStoragePlugin storage = (AbstractStoragePlugin)hbaseDesc.getDrillTable().getPlugin();
            String storageName = hbaseDesc.getDrillTable().getStorageEngineName();

            if(! mapCollection.containsKey(storageName)) {
                HBaseIndexCollection idxCollection =
                        new HBaseIndexCollection(storage, this.scanPrel, new HashSet<IndexDescriptor>());
                mapCollection.put(storageName, idxCollection);
            }
            mapCollection.get(storageName).addIndex(desc);
        }

        //XXX we don't support indexes on multiple storage plugins.
        if (mapCollection.size() > 0 ) {
            return mapCollection.values().iterator().next();
        }

        return null;
    }

    private IndexCollection getTableIndexFromMFS(String tableName) {
        return null;
    }
}
