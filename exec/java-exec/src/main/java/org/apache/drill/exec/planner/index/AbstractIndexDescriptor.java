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
import org.apache.drill.exec.physical.base.AbstractGroupScan;


/**
 * Abstract base class for Index descriptors
 *
 */
public abstract class AbstractIndexDescriptor implements IndexDescriptor {

  protected final List<SchemaPath> indexColumns;

  public AbstractIndexDescriptor(List<SchemaPath> cols) {
    indexColumns = cols;
  }

  @Override
  public Integer getIdIfValid(String fieldName) {
    SchemaPath schemaPath = SchemaPath.getSimplePath(fieldName);
    int id = indexColumns.indexOf(schemaPath);
    if (id == -1) {
      return null;
    }
    return id;
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
  public AbstractGroupScan getIndexGroupScan() {
    throw new UnsupportedOperationException("Group scan not supported for this index.");
  }

  @Override
  public boolean supportsFullTextSearch() {
    return false;
  }

}
