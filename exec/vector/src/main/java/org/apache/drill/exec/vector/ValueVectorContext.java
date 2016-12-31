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
package org.apache.drill.exec.vector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ValueVectorContext {
  private Map<ValueVector, Integer> vvActualSizes;
  private Map<ValueVector, Integer> vvExpectedSizes;

  public ValueVectorContext() {
    vvActualSizes = Collections.synchronizedMap(new HashMap<ValueVector, Integer>());
    vvExpectedSizes = Collections.synchronizedMap(new HashMap<ValueVector, Integer>());
  }

  public void setVVActualSize(ValueVector vv, int vvActualSize) {
    this.vvActualSizes.put(vv, vvActualSize);
  }

  public void setVVExpectedSize(ValueVector vv, int vvExpectedSize) {
    this.vvExpectedSizes.put(vv, vvExpectedSize);
  }

  public int getVVActualSize(ValueVector vv) {
    if(this.vvActualSizes.containsKey(vv)) {
      return vvActualSizes.get(vv);
    }
    return -1;
  }

  public int getVVExpectedSize(ValueVector vv) {
    if(this.vvExpectedSizes.containsKey(vv)) {
      return vvExpectedSizes.get(vv);
    }
    return -1;
  }

}
