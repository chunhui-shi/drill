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

import java.util.List;


public class IndexDesc {

    private String cluster;
    private String indexName;

    private List<IndexFieldDesc> indexedCols;
    private List<IndexFieldDesc> coveredCols;

        /**
         * Returns an ordered list of field descriptors which are indexed.
         */
        public List<IndexFieldDesc> getIndexedFields(){
            return indexedCols;
        }

        /**
         * Returns the list of additional fields that are stored in this Index but are
         * not indexed.
         */

        public List<IndexFieldDesc> getCoveredFields(){
            return coveredCols;
        }

        /**
         * Returns {@code true} if the values of the indexed field or the combination
         * of fields in case of composite index, are unique across all documents in
         * the table.
         */
        public boolean isUnique() {
            return true;
        }
        /**
         * Returns {@code true} if the index is not a native MapR-DB index.
         */

        public boolean isExternal() {
            return true;
        }

        /**
         * Returns {@code true} if the index is disabled.
         */

        public boolean isDisabled() {
            return false;
        }

        /**
         * Returns the fully qualified, canonical name of the index, e.g. path of
         * the native secondary index in MapR-DB.
         */

        public String getIndexName() {
            return indexName;
        }

        /**
         * Returns the name descriptor of the system where the index is hosted,
         * e.g. "maprdb" for native secondary index.
         */

        public String getSystem(){
            return "elasticsearch";
        }

        /**
         * Returns the cluster identifier of the system.
         */

        public String getCluster() {
            return cluster;
        }

        /**
         * For the external system that supports it, returns the connection string
         */
        public String getConnectionString() {
            return "";
        }

}
