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


import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;

import java.util.List;

public class HBaseIndexDescriptor extends AbstractIndexDescriptor {

    private DrillTable table;

    public HBaseIndexDescriptor(List<SchemaPath> indexCols,
                                List<SchemaPath> nonIndexCols,
                                List<SchemaPath> rowKeyColumns,
                                String indexName,
                                String tableName,
                                IndexDescriptor.IndexType type) {
        super(indexCols, nonIndexCols, rowKeyColumns, indexName, tableName, type);
    }



    public void setDrillTable(DrillTable table) {
        this.table = table;
    }

    public DrillTable getDrillTable() {
        return this.table;
    }


    public static HBaseIndexDescriptor getHBaseIndexDescriptor(IndexDesc indexFromFS, String tableName) {

        List<SchemaPath> indexCols = convertToSchemaPath( indexFromFS.getIndexedFields() );
        List<SchemaPath> nonIndexCols = convertToSchemaPath( indexFromFS.getCoveredFields() );
        List<SchemaPath> rowKeyColumns = null;

        return new HBaseIndexDescriptor(indexCols, nonIndexCols, rowKeyColumns, indexFromFS.getIndexName(), tableName,
                IndexDescriptor.IndexType.EXTERNAL_SECONDARY_INDEX);
    }


    public static List<SchemaPath> convertToSchemaPath(List<IndexFieldDesc> desc) {
        return null;
    }
}
