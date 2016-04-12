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
package org.apache.drill.exec.physical.impl.common;

import com.google.common.base.Preconditions;

import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Iterables;


public class HashTableIterator {

  // hash table on which to iterate
  final private HashTableTemplate hashTable;

  // current batch index within the hash table
  private int batchIndex;

  // current index within the current batch
  private int indexWithinBatch;

  private static enum HtIterState {
    NOT_STARTED,
    IN_PROCESS,
    DONE
  }

  private HtIterState state;

  public HashTableIterator(HashTableTemplate ht) {
    Preconditions.checkArgument(ht.batchHolders.size() > 0);
    this.hashTable = ht;
    this.batchIndex = 0;
    this.indexWithinBatch = 0;
    this.state = HtIterState.NOT_STARTED;
  }

  public boolean hasNext() {
    if (state == HtIterState.DONE) {
      return false;
    }
    if (batchIndex < hashTable.batchHolders.size()) {
      HashTableTemplate.BatchHolder bh = hashTable.batchHolders.get(batchIndex);
      return indexWithinBatch <= bh.maxOccupiedIdx;
    }
    return false;
  }

  public ValueHolder next(int colIndex) {
    if (state == HtIterState.DONE) {
      return null;
    }
    if (state == HtIterState.NOT_STARTED) {
      state = HtIterState.IN_PROCESS;
    }

    HashTableTemplate.BatchHolder bh = hashTable.batchHolders.get(batchIndex);

    // get the value vector corresponding to the given column index
    VectorWrapper<?> vw = Iterables.get(bh.htContainer, colIndex);
    ValueVector vv = vw.getValueVector();

    // get the entry at the current index within this batch
    ValueHolder value = ValueHolder.class.cast(vv.getAccessor().getObject(indexWithinBatch++));

    if (indexWithinBatch > bh.maxOccupiedIdx) {
      batchIndex++;
      indexWithinBatch = 0;
      if (batchIndex == hashTable.batchHolders.size()) {
        // no more data
        state = HtIterState.DONE;
      }
    }

    return value;
  }

}


