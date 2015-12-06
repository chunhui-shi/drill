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
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.hbase.HBaseGroupScan;

// Interface used to describe an index
public class HBaseSecondaryIndexDescriptor extends AbstractIndexDescriptor {
  private static final double DEFAULT_SELECTIVITY = 0.01;

  private final HBaseGroupScan hbscan;

  public HBaseSecondaryIndexDescriptor(PlannerSettings settings, ScanPrel scanPrel) {
    super(((HBaseGroupScan)scanPrel.getGroupScan()).getSecondaryIndexColumns());
    hbscan = (HBaseGroupScan)scanPrel.getGroupScan();
  }

  @Override
  public double getRows(RexNode indexCondition) {
    // TODO: Use the Elasticsearch COUNT API to compute the selectivity of the predicate
    // return row count based on default selectivity for now;
    return DEFAULT_SELECTIVITY * hbscan.getScanStats().getRecordCount();
  }

  @Override
  public AbstractGroupScan getIndexGroupScan() {
    // TODO Auto-generated method stub
    return null;
  }

}
