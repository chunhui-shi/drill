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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.GroupScan;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Abstract base class for Index collection (collection of Index descriptors)
 *
 */
public abstract class AbstractIndexCollection implements IndexCollection {

  /**
   * A set of indexes for a particular table
   */
  protected Set<IndexDescriptor> indexSet;

  public AbstractIndexCollection() {
    indexSet = Sets.newHashSet();
  }

  @Override
  public boolean addIndex(IndexDescriptor index) {
    return indexSet.add(index);
  }

  @Override
  public boolean removeIndex(IndexDescriptor index) {
    return indexSet.remove(index);
  }

  @Override
  public void clearAll() {
    indexSet.clear();
  }

  @Override
  public boolean supportsIndexSelection() {
    return false;
  }

  @Override
  public double getRows(RexNode indexCondition) {
    throw new UnsupportedOperationException("getRows() not supported for this index collection.");
  }

  @Override
  public boolean supportsRowCountStats() {
    return false;
  }

  @Override
  public boolean supportsFullTextSearch() {
    return false;
  }

  @Override
  public GroupScan getGroupScan() {
    return null;
  }

  @Override
  public boolean isColumnIndexed(String fieldName) {
    for (IndexDescriptor index : indexSet) {
      if (index.getIndexColumnOrdinal(fieldName) == 0) {
        return true;
      }
    }
    return false;
  }

}
