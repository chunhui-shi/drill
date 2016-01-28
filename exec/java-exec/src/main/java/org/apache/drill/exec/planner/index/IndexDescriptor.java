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
import org.apache.drill.exec.physical.base.AbstractGroupScan;

// Interface used to describe an index
public interface IndexDescriptor {
  /**
   * Types of an index: PRIMARY_KEY_INDEX, NATIVE_SECONDARY_INDEX, EXTERNAL_SECONDARY_INDEX
   */
  public static enum IndexType {
    PRIMARY_KEY_INDEX,
    NATIVE_SECONDARY_INDEX,
    EXTERNAL_SECONDARY_INDEX
  };

  /**
   * Check to see if the name is an index column
   * @param name The field name you want to compare to index names.
   * @return Return index of the indexed column if valid, otherwise return null;
   */
  public Integer getIdIfValid(String name);

  /**
   * Get the type of this index based on {@link IndexType}
   * @return one of the values in {@link IndexType}
   */
  public IndexType getIndexType();

  /**
   * Get the estimated row count for a single index condition
   * @param indexCondition The index condition (e.g index_col1 < 10 AND index_col2 = 'abc')
   * @return The estimated row count
   */
  public double getRows(RexNode indexCondition);

  /**
   * Whether or not the index supports getting row count statistics
   * @return True if index supports getting row count, False otherwise
   */
  public boolean supportsRowCountStats();

  /**
   * Get an instance of the group scan associated with this index descriptor
   * @return An instance of group scan for this index
   */
  public AbstractGroupScan getIndexGroupScan();

  /**
   * Whether or not the index supports full-text search (to allow pushing down such filters)
   * @return True if index supports full-text search, False otherwise
   */
  public boolean supportsFullTextSearch();

}
