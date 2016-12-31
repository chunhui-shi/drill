/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.record;

import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.ValueVectorVisitor;
import org.apache.drill.exec.vector.ZeroVector;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.UnionVector;

class BufSizeResult {
  public boolean correct;
  public int actualSize;
  public int expectedSize;

  public BufSizeResult(boolean isCorrect, int actual, int expected) {
    correct = isCorrect;
    actualSize = actual;
    expectedSize = expected;
  }

  public BufSizeResult() {
    this(false,0,0);
  }

  public BufSizeResult add(BufSizeResult that) {
    actualSize += that.actualSize;
    expectedSize += that.expectedSize;
    correct = correct==false? false: (actualSize == expectedSize);
    return this;
  }
}

public class VVBufSizeVerifier implements ValueVectorVisitor<BufSizeResult> {

  @Override
  public BufSizeResult visitBaseData(BaseDataValueVector vv) {
    return new BufSizeResult(true, vv.getBufferSize(), vv.getBufferSize());
  }

  @Override
  public BufSizeResult visitBaseRepeated(BaseRepeatedValueVector vv) {
    return new BufSizeResult(true, vv.getBufferSize(), vv.getBufferSize());
  }

  @Override
  public BufSizeResult visitObject(ObjectVector vv) {
    return new BufSizeResult(true, 0, 0);
  }

  @Override
  public BufSizeResult  visitMap(MapVector vv) {
    BufSizeResult ret = new BufSizeResult(true, 0, 0);
    if (vv.getAccessor().getValueCount() == 0 || vv.size() == 0) {
      return ret;
    }

    for (final ValueVector v : vv) {
      ret.add(v.accept(this));
      if(ret.correct == false) {
        return ret;
      }
    }
    return ret;
  }

  @Override
  public BufSizeResult  visitUnion(UnionVector vv) {
    return visitMap(vv.getInternalMap());
  }

  @Override
  public BufSizeResult  visitZero(ZeroVector vv) {
    return new BufSizeResult(true, 0, 0);
  }

  @Override
  public BufSizeResult visitRepeatedMap(RepeatedMapVector vv ) {
    return new BufSizeResult();
  }

  @Override
  public BufSizeResult visitList(ListVector vv ) {
    return new BufSizeResult();
  }

  @Override
  public BufSizeResult visitRepeatedList(RepeatedListVector vv ) {
    return new BufSizeResult();
  }
}
