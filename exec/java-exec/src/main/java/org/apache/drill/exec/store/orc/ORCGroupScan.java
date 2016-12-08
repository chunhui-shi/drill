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
package org.apache.drill.exec.store.orc;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractFileGroupScan;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.ParquetFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import java.io.IOException;
import java.util.List;

public class ORCGroupScan extends AbstractFileGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ORCGroupScan.class);

  @JsonCreator
  public ORCGroupScan(@JsonProperty("userName") String userName,
                      @JsonProperty("entries") List<ReadEntryWithPath> entries,//
                      @JsonProperty("storage") StoragePluginConfig storageConfig, //
                      @JsonProperty("format") FormatPluginConfig formatConfig, //
                      @JacksonInject StoragePluginRegistry engineRegistry, //
                      @JsonProperty("columns") List<SchemaPath> columns, //
                      @JsonProperty("selectionRoot") String selectionRoot, //
                      @JsonProperty("cacheFileRoot") String cacheFileRoot, //
                      @JsonProperty("filter") LogicalExpression filter
  ) {
    super(userName);
  }

  public ORCGroupScan(ORCGroupScan that) {
    super(that.getUserName());
  }
  public ORCGroupScan( //
                           String userName,
                           FileSelection selection, //
                           ORCFormatPlugin formatPlugin, //
                           String selectionRoot,
                           String cacheFileRoot,
                           List<SchemaPath> columns) throws IOException{
    this(userName, selection, formatPlugin, selectionRoot, cacheFileRoot, columns, ValueExpressions.BooleanExpression.TRUE);
  }

  public ORCGroupScan( //
                           String userName,
                           FileSelection selection, //
                           ORCFormatPlugin formatPlugin, //
                           String selectionRoot,
                           String cacheFileRoot,
                           List<SchemaPath> columns,
                           LogicalExpression filter) {
    super(userName);
  }
  @Override
  public void modifyFileSelection(FileSelection selection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileGroupScan clone(FileSelection selection) throws IOException {
    throw new UnsupportedOperationException();
  }


  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {

  }

  public SubScan getSpecificScan(int minorFragmentId)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ORCGroupScan(this);
  }


  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "ORCGroupScan";
  }
}