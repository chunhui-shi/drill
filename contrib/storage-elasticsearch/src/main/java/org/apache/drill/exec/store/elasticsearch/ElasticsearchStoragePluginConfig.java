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
package org.apache.drill.exec.store.elasticsearch;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import org.apache.commons.lang3.tuple.ImmutablePair;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_HOSTS;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_CLUSTER;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_BATCH_SIZE;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_DEFAULT_BATCH_SIZE;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_SCROLL_TIMEOUT;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_DEFAULT_SCROLL_TIMEOUT;

/**
 * Storage plugin config for elasticsearch.
 */
@JsonTypeName(ElasticsearchStoragePluginConfig.NAME)
public class ElasticsearchStoragePluginConfig extends StoragePluginConfigBase {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchStoragePluginConfig.class);

    public static final String NAME = "elasticsearch";

    private Map<String, String> config;
    private TransportClient     client;

    @JsonCreator
    public ElasticsearchStoragePluginConfig(@JsonProperty("config") Map<String, String> config) {

        this.config = config;

        checkArgument(config.containsKey(ES_CONFIG_KEY_CLUSTER), "Missing property: " + ES_CONFIG_KEY_CLUSTER);
        checkArgument(config.containsKey(ES_CONFIG_KEY_HOSTS), "Missing property: " + ES_CONFIG_KEY_HOSTS);

        if (!config.containsKey(ES_CONFIG_KEY_BATCH_SIZE)) {
            this.config.put(ES_CONFIG_KEY_BATCH_SIZE, ES_CONFIG_DEFAULT_BATCH_SIZE);
        }

        logger.info("Elasticsearch storage plugin config: [{}]",
                Joiner.on(", ").withKeyValueSeparator(":").join(this.config));
    }

    @JsonProperty
    public Map<String, String> getConfig() {
        return ImmutableMap.copyOf(config);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ElasticsearchStoragePluginConfig that = (ElasticsearchStoragePluginConfig) o;
        return config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return config.hashCode();
    }

    @JsonIgnore
    public String getClusterName() {
        return config.get(ES_CONFIG_KEY_CLUSTER);
    }

    @JsonIgnore
    public int getBatchSize() {
        String s = config.get(ES_CONFIG_KEY_BATCH_SIZE);
        if (s == null || s.isEmpty()) {
            return Integer.parseInt(ES_CONFIG_DEFAULT_BATCH_SIZE);
        }
        return Integer.parseInt(s);
    }

    @JsonIgnore
    public long getScrollTimeout() {
        String s = config.get(ES_CONFIG_KEY_SCROLL_TIMEOUT);
        if (s == null || s.isEmpty()) {
            return Integer.parseInt(ES_CONFIG_DEFAULT_SCROLL_TIMEOUT);
        }
        return Integer.parseInt(s);
    }

    @JsonIgnore
    private List<ImmutablePair<String, Integer>> getHosts() {

        String hosts = config.get(ES_CONFIG_KEY_HOSTS);
        ImmutableList.Builder<ImmutablePair<String, Integer>> builder = ImmutableList.builder();

        for (String host : Splitter.on(",").omitEmptyStrings().trimResults().split(hosts)) {

            List<String> elements = new ArrayList<>();
            Iterables.addAll(elements, Splitter.on(":").omitEmptyStrings().trimResults().split(host));

            if (elements.size() == 0 || elements.size() > 2) {
                logger.warn("Invalid host:port pair: " + host);
                continue;
            }

            Integer port = null;
            if (elements.size() == 2) {
                try {
                    port = Integer.valueOf(elements.get(1));
                } catch (NumberFormatException e) {
                    logger.warn("Invalid host:port pair: " + host);
                    continue;
                }
            }

            builder.add(ImmutablePair.of(elements.get(0), port));
        }

        return builder.build();
    }

    @JsonIgnore
    public synchronized Client getClient() {

        if (client == null) {
            client = TransportClient.builder().settings(Settings.builder()
                            .put("path.home", "/tmp")   // XXX - remove this hack
                            .put("cluster.name", getClusterName()))
                            .build();

            for (ImmutablePair<String, Integer> pair : getHosts()) {
                client.addTransportAddress(
                        new InetSocketTransportAddress(pair.getLeft(), pair.getRight()));
            }
        }

        return client;
    }
}
