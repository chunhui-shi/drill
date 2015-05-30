/*
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
package org.apache.drill.exec.store.elasticsearch;

import com.google.common.base.Preconditions;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchSubScan.ElasticsearchSubScanSpec;
import org.apache.drill.exec.store.elasticsearch.search.SearchOptions;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Elasticsearch group scan.
 */
@JsonTypeName("elasticsearch-scan")
public class ElasticsearchGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchGroupScan.class);

    private final ElasticsearchScanSpec      spec;
    private final List<SchemaPath>           columns;
    private final ElasticsearchStoragePlugin plugin;

    private boolean filterPushedDown = false;

    @JsonCreator
    public ElasticsearchGroupScan(@JsonProperty("userName") String userName,
                                  @JsonProperty("elasticsearchScanSpec") ElasticsearchScanSpec spec,
                                  @JsonProperty("storage") ElasticsearchStoragePluginConfig config,
                                  @JsonProperty("columns") List<SchemaPath> columns,
                                  @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {

        this(userName, (ElasticsearchStoragePlugin) pluginRegistry.getPlugin(config), spec, columns);
    }

    public ElasticsearchGroupScan(String userName, ElasticsearchStoragePlugin plugin, ElasticsearchScanSpec spec,
                                  List<SchemaPath> columns) {
        super(userName);
        this.plugin  = plugin;
        this.spec    = spec;
        this.columns = (columns == null || columns.size() == 0) ? ALL_COLUMNS : columns;
    }

    private ElasticsearchGroupScan(ElasticsearchGroupScan other, List<SchemaPath> columns) {
        super(other);
        this.plugin  = other.plugin;
        this.spec    = other.spec;
        this.filterPushedDown = other.filterPushedDown;
        this.columns = (columns == null || columns.size() == 0) ? ALL_COLUMNS : columns;
    }

    private ElasticsearchGroupScan(ElasticsearchGroupScan other) {
        super(other);
        this.plugin  = other.plugin;
        this.spec    = other.spec;
        this.columns = other.columns;
        this.filterPushedDown = other.filterPushedDown;
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        return new ElasticsearchGroupScan(this, columns);
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints)
            throws PhysicalOperatorSetupException {
        // XXX - This needs to be properly implemented.
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {

        logger.info("Creating sub-scan on [{}.{}:{}] for minor fragment [{}]",
                spec.getSchema(), spec.getTable(), columns, minorFragmentId);

        SearchOptions options = new SearchOptions(
                plugin.getConfig().getBatchSize(), plugin.getConfig().getScrollTimeout());

        ElasticsearchSubScanSpec sub = new ElasticsearchSubScanSpec(
                spec.getSchema(), spec.getTable(), spec.getQuery(), options);

        LinkedList<ElasticsearchSubScanSpec> subs = new LinkedList<>();
        subs.add(sub);

        return new ElasticsearchSubScan(getUserName(), plugin, subs, columns);
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        // XXX - This needs to be properly implemented.
        return super.getOperatorAffinity();
    }

    @Override
    @JsonIgnore
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new ElasticsearchGroupScan(this);
    }

    @Override
    public int getMaxParallelizationWidth() {
        // XXX - This needs to be properly implemented.
        return 1;
    }

    @Override
    public ScanStats getScanStats() {
        // XXX - This needs to be properly implemented.
        return ScanStats.TRIVIAL_TABLE;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public String toString() {
        return "ElasticsearchGroupScan [ElasticsearchScanSpec="
                + spec + ", columns="
                + columns + "]";
    }

    @JsonProperty("storage")
    public ElasticsearchStoragePluginConfig getStorageConfig() {
        return plugin.getConfig();
    }

    @JsonIgnore
    public ElasticsearchStoragePlugin getStoragePlugin() {
        return plugin;
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty
    public ElasticsearchScanSpec getScanSpec() {
        return spec;
    }

    @JsonIgnore
    public boolean isFilterPushedDown() {
        return filterPushedDown;
    }

    @JsonIgnore
    public void setFilterPushedDown(boolean isFilterPushedDown) {
        this.filterPushedDown = isFilterPushedDown;
    }

    @Override
    @JsonIgnore
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }
}
