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

//temp replace of the same name interface of mfs.
public class IndexFieldDesc {
    public static enum Order {
        None,
        Asc,
        Desc;
    }

    private String field;
    private Order fieldOrder;

    public IndexFieldDesc(String inField){
        field = inField;
        fieldOrder = Order.None;
    }
    public IndexFieldDesc(String inField, Order inOrder) {
        field = inField;
        fieldOrder = inOrder;
    }

    /**
     * Returns the canonical field name of this field descriptor.
     */
    public String getFieldName(){
        return field;
    }
    /**
     * Returns the ordering scheme of this field in the Index.
     */

    public Order getSortOrder(){
        return fieldOrder;
    }

    /**
     * Return {@code true} if this is a functional index field.
     */

    public boolean isFunctional(){
        return true;
    }
}
