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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableSet;

import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.elasticsearch.rules.ElasticsearchPushFilterIntoScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Storage plugin for elasticsearch.
 */
public class ElasticsearchStoragePlugin extends AbstractStoragePlugin {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchStoragePlugin.class);

    private final DrillbitContext                  context;
    private final ElasticsearchSchemaFactory       factory;
    private final ElasticsearchStoragePluginConfig config;

    public ElasticsearchStoragePlugin(ElasticsearchStoragePluginConfig config, DrillbitContext context, String name) {
        this.config  = config;
        this.context = context;
        this.factory = new ElasticsearchSchemaFactory(this, name);
    }

    @Override
    public ElasticsearchStoragePluginConfig getConfig() {
        return config;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        factory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        return getPhysicalScan(userName, selection, null);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
            throws IOException {
        ElasticsearchScanSpec spec =
                selection.getListWith(new ObjectMapper(), new TypeReference<ElasticsearchScanSpec>() {});
        logger.info("Created new physical scan on [{}.{}]", spec.getSchema(), spec.getTable());
        return new ElasticsearchGroupScan(userName, this, spec, columns);
    }

    @Override
    public Set<StoragePluginOptimizerRule> getOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
        return ImmutableSet.of(
                ElasticsearchPushFilterIntoScan.FILTER_ON_PROJECT,
                ElasticsearchPushFilterIntoScan.FILTER_ON_SCAN);
    }
}
