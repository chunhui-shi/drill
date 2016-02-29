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

import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;

import com.google.common.collect.Lists;

/**
 * Abstract base class for an Index descriptor
 *
 */
public abstract class AbstractIndexDescriptor implements IndexDescriptor {

  /**
   * The indexColumns is the list of column(s) on which this index is created. If there is more than 1 column,
   * the order of the columns is important: index on {a, b} is not the same as index on {b, a}
   * NOTE: the indexed column could be of type columnfamily.column
   */
  protected final List<SchemaPath> indexColumns;

  /**
   * nonIndexColumns: the list of columns that are included in the index as 'covering'
   * columns but are not themselves indexed.  These are useful for covering indexes where the
   * query request can be satisfied directly by the index and avoid accessing the table altogether.
   */
  protected final List<SchemaPath> nonIndexColumns;

  /**
   * indexName: name of the index that should be unique within the scope of a table
   */
  protected final String indexName;

  protected final String tableName;

  protected final IndexDescriptor.IndexType indexType;

  public AbstractIndexDescriptor(List<SchemaPath> indexCols,
      List<SchemaPath> nonIndexCols,
      String indexName,
      String tableName,
      IndexDescriptor.IndexType type) {
    this.indexColumns = indexCols;
    this.nonIndexColumns = nonIndexCols;
    this.indexName = indexName;
    this.tableName = tableName;
    this.indexType = type;
  }

  @Override
  public int getIndexColumnOrdinal(String fieldName) {
    SchemaPath schemaPath = SchemaPath.getSimplePath(fieldName);
    int id = indexColumns.indexOf(schemaPath);
    return id;
  }

  @Override
  public boolean isCoveringIndex(List<SchemaPath> columns) {
    List<SchemaPath> allColumns = Lists.newArrayList();
    allColumns.addAll(indexColumns);
    allColumns.addAll(nonIndexColumns);
    return allColumns.contains(columns);
  }

  @Override
  public String toString() {
    String columnsDesc = " Index columns: " + indexColumns.toString() + " Non-Index columns: " + nonIndexColumns.toString();
    String desc = "Table: " + tableName + " Index: " + indexName + columnsDesc;
    return desc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    AbstractIndexDescriptor index1 = (AbstractIndexDescriptor) o;
    return tableName.equals(index1.tableName)
        && indexName.equals(index1.indexName)
        && indexType.equals(index1.indexType)
        && indexColumns.equals(index1.indexColumns) ;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    final String fullName = tableName + indexName;
    int result = 1;
    result = prime * result + fullName.hashCode();
    result = prime * result + indexType.hashCode();

    return result;
  }

  @Override
  public String getIndexName() {
    return indexName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public IndexDescriptor.IndexType getIndexType() {
    return indexType;
  }

  @Override
  public double getRows(RexNode indexCondition) {
    throw new UnsupportedOperationException("getRows() not supported for this index.");
  }

  @Override
  public boolean supportsRowCountStats() {
    return false;
  }

  @Override
  public GroupScan getIndexGroupScan() {
    throw new UnsupportedOperationException("Group scan not supported for this index.");
  }

  @Override
  public boolean supportsFullTextSearch() {
    return false;
  }

}
