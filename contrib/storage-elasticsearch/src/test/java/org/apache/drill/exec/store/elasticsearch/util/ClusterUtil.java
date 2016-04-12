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
package org.apache.drill.exec.store.elasticsearch.util;

import com.google.common.io.Files;

import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePluginConfig;
import org.apache.drill.exec.store.elasticsearch.types.ElasticsearchType;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.transport.TransportInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Closeable;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_HOSTS;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_CLUSTER;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_KEY_BATCH_SIZE;
import static org.apache.drill.exec.store.elasticsearch.ElasticsearchConstants.ES_CONFIG_DEFAULT_BATCH_SIZE;


/**
 * Utilities for testing against embedded or remote elasticsearch clusters.
 */
public final class ClusterUtil implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterUtil.class);

    public static final String CLUSTER_NAME = "drill-test-cluster";

    private Node            node;
    private TransportClient remote;
    private String          remoteClusterName;

    public static final EnumSet<ElasticsearchType> ALL_TYPES = EnumSet.allOf(ElasticsearchType.class);

    public static final EnumSet<ElasticsearchType> PRIMITIVE_TYPES = EnumSet.of(
            ElasticsearchType.INTEGER,
            ElasticsearchType.LONG,
            ElasticsearchType.FLOAT,
            ElasticsearchType.DOUBLE,
            ElasticsearchType.BOOLEAN,
            ElasticsearchType.STRING);

    public static final EnumSet<ElasticsearchType> NESTED_TYPES = EnumSet.of(
            ElasticsearchType.NESTED,
            ElasticsearchType.OBJECT);

    /**
     * Starts a single-node elasticsearch cluster.
     */
    public ClusterUtil() {
        logger.info("--> initializing elasticsearch cluster");
        File data = Files.createTempDir();
        data.deleteOnExit();
        File home = Files.createTempDir();
        home.deleteOnExit();
        Settings settings = Settings.builder()
                .put("path.home", home.getAbsolutePath())
                .put("path.data", data.getAbsolutePath())
                .build();
        node = nodeBuilder().settings(settings).local(false).data(true).clusterName(CLUSTER_NAME).node();
    }

    /**
     * Connects to a remote elasticsearch cluster.
     */
    public ClusterUtil(String cluster, String host, int port) {
        logger.info("--> connecting to elasticsearch cluster: [{}:{}:{}]", cluster, host, port);
        File home = Files.createTempDir();
        home.deleteOnExit();
        remote = TransportClient.builder()
                .settings(Settings.builder().put("cluster.name", cluster)
                        .put("path.home", home.getAbsolutePath()).build()).build();
        try {
            remote.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        }
        catch(UnknownHostException e){
            logger.error("unknown host: " + host);
        }
        remoteClusterName = cluster;
    }

    /**
     * Creates a storage plugin config with values suitable for creating
     * connections to the embedded elasticsearch cluster.
     */
    public ElasticsearchStoragePluginConfig config() {

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        TransportInfo info = response.getNodes()[0].getTransport();
        InetSocketTransportAddress inet = (InetSocketTransportAddress) info.address().publishAddress();

        Map<String, String> map = new HashMap<>();
        map.put(ES_CONFIG_KEY_HOSTS, inet.address().getAddress().getHostAddress() + ":" + inet.address().getPort());
        map.put(ES_CONFIG_KEY_CLUSTER, remoteClusterName != null ? remoteClusterName : CLUSTER_NAME);
        map.put(ES_CONFIG_KEY_BATCH_SIZE, ES_CONFIG_DEFAULT_BATCH_SIZE);

        ElasticsearchStoragePluginConfig config = new ElasticsearchStoragePluginConfig(map);
        config.setEnabled(true);
        return config;
    }

    /**
     * Gets a client to the embedded cluster.
     */
    public Client client() {
        return node == null ? remote : node.client();
    }

    /**
     * Wipes cluster clean of all indices.
     */
    public void wipe() {
        try {
            client().admin().indices().prepareDelete("*").execute().actionGet();
        } catch (Exception e) {
            logger.warn("--> failed to wipe test indices");
        }
    }

    /**
     * Shuts down the embedded cluster.
     */
    @Override
    public void close() throws IOException {
        logger.info("--> tearing down elasticsearch cluster");
        if (node != null) {
            node.close();
        }
    }

    /**
     * Creates a table in the given schema with the given name with fields for
     * all supported data types.
     */
    public void table(String schema, String... tables) throws IOException {
        for (String table : tables) {
            table(schema, table, ALL_TYPES);
        }
    }

    /**
     * Creates a table in the given schema with the given name with fields for
     * all supported data types.
     */
    public void table(String schema, String table, EnumSet<ElasticsearchType> types) throws IOException {

        schema(schema);

        PutMappingRequestBuilder put = client().admin().indices().preparePutMapping(schema);
        put.setType(table);

        XContentBuilder json = XContentFactory.jsonBuilder();
        json.startObject().startObject(table).startObject("properties");

        for (ElasticsearchType type : types) {
            switch (type) {
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                case STRING:
                    String name = type.name().toLowerCase(Locale.ENGLISH);
                    json.startObject(name + "_field");
                    json.field("type", name);
                    json.endObject();
                    break;
                case NESTED:
                    json.startObject("person").field("type", "nested").startObject("properties");
                    json.startObject("first_name").field("type", "string").endObject();
                    json.startObject("last_name").field("type", "string").endObject();
                    json.startObject("ssn").field("type", "integer").endObject();

                    json.startObject("address").field("type", "nested").startObject("properties");
                    json.startObject("street_line_1").field("type", "string").field("index", "not_analyzed").endObject();
                    json.startObject("street_line_2").field("type", "string").field("index", "not_analyzed").endObject();
                    json.startObject("city").field("type", "string").field("index", "not_analyzed").endObject();
                    json.startObject("state").field("type", "string").field("index", "not_analyzed").endObject();
                    json.startObject("zipcode").field("type", "integer").endObject();
                    json.endObject();
                    json.endObject();

                    json.startObject("relative").field("type", "nested").startObject("properties");
                    json.startObject("first_name").field("type", "string").endObject();
                    json.startObject("last_name").field("type", "string").endObject();
                    json.endObject();
                    json.endObject();
                    json.endObject();

                    json.endObject();
                    break;
                case OBJECT:
                    json.startObject("person2").field("type", "nested").startObject("properties");
                    json.startObject("first_name").field("type", "string").endObject();
                    json.startObject("last_name").field("type", "string").endObject();
                    json.startObject("ssn").field("type", "integer").endObject();
                    json.endObject().endObject();
                    break;
                case GEO_POINT:
                case GEO_SHAPE:
                    break;
            }
        }

        json.endObject().endObject().endObject();

        put.setSource(json);
        put.execute().actionGet();
        green();
    }

    /**
     * Creates schemas with the given name(s).
     */
    public void schema(String... schemas) {
        for (String schema : schemas) {

            IndicesExistsResponse response = client().admin().indices().prepareExists(schema).execute().actionGet();
            if (response.isExists()) {
                continue;
            }

            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            client().admin().indices().prepareCreate(schema).setSettings(settings).execute().actionGet();
        }
        green();
    }

    /**
     * Creates an alias to the given schema(s).
     */
    public void alias(String alias, String... schemas) {
        client().admin().indices().prepareAliases().addAlias(schemas, alias).execute().actionGet();
        green();
    }

    /**
     * Populates the given index with test data.
     */
    public void populate(String schema, String table, int rows) throws IOException {
        populate(schema, table, rows, ALL_TYPES);
    }

    public void populate(String schema, String table, int rows, EnumSet<ElasticsearchType> types) throws IOException {
        populate(schema, table, rows, false, types);
    }

    /**
     * Populates the given index with test data.
     */
    public void populate(String schema, String table, int rows, boolean arrays,
                         EnumSet<ElasticsearchType> types) throws IOException {

        table(schema, table, types);

        int numTestValues = arrays ? 5 : 1;

        BulkRequestBuilder bulk = client().prepareBulk();

        for (int i = 0; i < rows; i++) {

            IndexRequestBuilder builder = client().prepareIndex().setIndex(schema).setType(table);
            XContentBuilder json = XContentFactory.jsonBuilder().startObject();

            for (ElasticsearchType type : types) {
                switch (type) {
                    case INTEGER:
                    case LONG:
                        int[] ints = new int[numTestValues];
                        for (int j = 0; j < numTestValues; j++) {
                            ints[j] = j;
                        }
                        for (int j = 0; j < numTestValues; j++) {
                            json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", ints);
                        }
                        break;
                    case FLOAT:
                        float[] floats = new float[numTestValues];
                        for (int j = 0; j < numTestValues; j++) {
                            floats[j] = (float) j;
                        }
                        for (int j = 0; j < numTestValues; j++) {
                            json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", floats);
                        }
                        break;
                    case DOUBLE:
                        double[] doubles = new double[numTestValues];
                        for (int j = 0; j < numTestValues; j++) {
                            doubles[j] = (double) j;
                        }
                        for (int j = 0; j < numTestValues; j++) {
                            json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", doubles);
                        }
                        break;
                    case BOOLEAN:
                        boolean[] booleans = new boolean[numTestValues];
                        for (int j = 0; j < numTestValues; j++) {
                            booleans[j] = i % 2 == 0;
                        }
                        for (int j = 0; j < numTestValues; j++) {
                            json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", booleans);
                        }
                        break;
                    case STRING:
                        String[] strings = new String[numTestValues];
                        for (int j = 0; j < numTestValues; j++) {
                            strings[j] = "string_value_" + Integer.toString(i) + Integer.toString(j);
                        }
                        for (int j = 0; j < numTestValues; j++) {
                            json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", strings);
                        }
                        break;
                    case NESTED:
                        json.startObject("person");
                        json.field("first_name", "my_first_name_" + i);
                        json.field("last_name", "my_last_name_" + i);
                        json.field("ssn", 1234 + i);

                        json.startObject("address");
                        json.field("street_line_1", i + " main st.");
                        json.field("street_line_2", "#" + i);
                        if (i % 2 == 0) {
                            json.field("city", "seattle");
                        } else {
                            json.field("city", "oxford");
                        }
                        json.field("zipcode", i);
                        json.endObject();

                        json.startObject("relative");
                        json.field("first_name", "relatives_first_name_" + i);
                        json.field("last_name", "relatives_last_name_" + i);
                        json.endObject();

                        json.endObject();
                        break;
                    case OBJECT:
                        json.startObject("person2");
                        json.field("first_name", "my_first_name_" + i);
                        json.field("last_name", "my_last_name_" + i);
                        json.field("ssn", 1234 + i);
                        json.endObject();
                        break;
                    case GEO_POINT:
                        // XXX - Implement
                    case GEO_SHAPE:
                        // XXX - Implement
                        break;
                    default:
                        break;
                }
            }

            builder.setSource(json.endObject());
            bulk.add(builder.request());
        }

        bulk.setRefresh(true);
        BulkResponse response = bulk.execute().actionGet();
        assertFalse(response.hasFailures());

        logger.info("--> indexed [{}] test documents", rows);
    }

    /**
     * Waits for cluster to attain green state.
     */
    public void green() {

        TimeValue timeout = TimeValue.timeValueSeconds(30);

        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().
                        timeout(timeout).
                        waitForGreenStatus().
                        waitForEvents(Priority.LANGUID).
                        waitForRelocatingShards(0)).actionGet();

        if (actionGet.isTimedOut()) {
            logger.info("--> timed out waiting for cluster green state.\n{}\n{}",
                    client().admin().cluster().prepareState().get().getState().prettyPrint(),
                    client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            fail("timed out waiting for cluster green state");
        }

        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }
}
