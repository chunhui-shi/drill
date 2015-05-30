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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.store.elasticsearch.random.ReproducibleListener;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.elasticsearch.util.ClusterUtil;
import org.apache.drill.common.exceptions.ExecutionSetupException;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;


/**
 * Abstract base class for all elasticsearch test classes.
 *
 * By default, tests will start an embedded elasticsearch single-node
 * cluster. An external cluster may be used by specifying the following
 * properties on the command line:
 *
 *  - elasticsearch.test.cluster.name : remote cluster name
 *  - elasticsearch.test.cluster.addr : host:port of remote cluster
 *  - elasticsearch.test.cluster.wipe : whether or not to clear test results
 *
 */
@Listeners(ReproducibleListener.class)
@RunWith(RandomizedRunner.class)

// XXX - CHANGE THIS TO SUTIE OR TEST
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)

public abstract class ElasticsearchTestBase extends BaseTestQuery {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTestBase.class);

    // Settings for running tests against a remote elasticsearch cluster
    private static String remoteClusterName  = System.getProperty("elasticsearch.test.cluster.name", "");
    private static String remoteClusterAddr  = System.getProperty("elasticsearch.test.cluster.addr", "");
    private static boolean remoteClusterWipe = Boolean.getBoolean("elasticsearch.test.cluster.wipe");

    private static boolean remote = false;

    protected static ClusterUtil cluster;

    protected static RandomizedContext randomizedContext;

    @BeforeClass
    public static void init() throws ExecutionSetupException {

        // Configure embedded or remote elasticsearch cluster

        if (!remoteClusterAddr.isEmpty() && !remoteClusterName.isEmpty()) {
            String[] elements = remoteClusterAddr.split(":");
            if (elements.length != 2) {
                throw new RuntimeException("Invalid remote cluster coordinates");
            }
            remote = true;
            cluster = new ClusterUtil(remoteClusterName, elements[0], Integer.valueOf(elements[1]));
        } else {
            cluster = new ClusterUtil();
        }

        // Configure storage plugin

        StoragePluginRegistry registry = getDrillbitContext().getStorage();
        registry.createOrUpdate(ElasticsearchStoragePluginConfig.NAME, cluster.config(), true);
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (cluster != null) {
            wipe();
            cluster.close();
        }
    }

    @Before
    public void before() {
        ;
    }

    @After
    public void after() {
        wipe();
    }

    protected Random random() {
        return RandomizedContext.current().getRandom();
    }

    protected static void wipe() {
        if (cluster != null) {
            if (!remote || remoteClusterWipe) {
                cluster.wipe();
            }
        }
    }

    /**
     * Display query results in a simple tabular format.
     */
    public static void display(List<QueryDataBatch> batch) throws SchemaChangeException {

        StringBuilder sb = new StringBuilder();
        RecordBatchLoader loader = new RecordBatchLoader(getAllocator());

        for (QueryDataBatch result : batch) {

            loader.load(result.getHeader().getDef(), result.getData());
            if (loader.getRecordCount() <= 0) {
                continue;
            }

            VectorUtil.appendVectorAccessibleContent(loader, sb, ", ", true);
            loader.clear();
            result.release();
        }

        logger.info("\n--- Query Results ---\n{}", sb.toString());
    }
}
