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
package org.apache.drill.exec.store.elasticsearch.search;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import org.apache.drill.common.expression.SchemaPath;

import org.elasticsearch.client.Client;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Manages executing search queries against elasticsearch.
 */
public class Searcher {

    private static final Logger logger = LoggerFactory.getLogger(Searcher.class);

    private final String schema;
    private final String table;
    private final Client client;
    private final SearchOptions options;
    private final QueryBuilder query;

    private long totalHitCount = 0;
    private long totalProcessedCount = 0;
    private boolean searchInProgress = false;
    private String[] includes = null;
    private ListenableActionFuture<SearchResponse> future;

    public Searcher(Client client, String schema, String table, Collection<SchemaPath> columns, String queryString,
                    SearchOptions options) {

        this.client  = Preconditions.checkNotNull(client);
        this.schema  = Preconditions.checkNotNull(schema);
        this.table   = Preconditions.checkNotNull(table);
        this.options = Preconditions.checkNotNull(options);

        Preconditions.checkArgument(columns != null && columns.size() > 0);

        Collection<String> paths = Collections2.transform(columns, new Function<SchemaPath, String>() {
            @Override
            public String apply(SchemaPath input) {
                return input.getAsUnescapedPath();
            }
        });
        includes = paths.toArray(new String[paths.size()]);

        if (queryString != null && !queryString.isEmpty()) {
            WrapperQueryBuilder wrapper = new WrapperQueryBuilder(queryString);
            query = boolQuery().filter(constantScoreQuery(wrapper));
        }
        else {
            query = constantScoreQuery(matchAllQuery());
        }
    }

    public void search() {

        SearchRequestBuilder request = client.prepareSearch(schema)
                .setTypes(table)
                .setScroll(options.getScrollTimeout())
                .setSize(options.getBatchSize());

        if (includes != null) {
            request.setFetchSource(includes, null);
        }

        request.setQuery(query);

        future = request.execute();
        searchInProgress = true;
    }

    public void search(String scrollId) {

        SearchScrollRequestBuilder request = client.prepareSearchScroll(scrollId).setScroll(options.getScrollTimeout());
        future = request.execute();

        searchInProgress = true;
    }

    public SearchResponse response() {

        Preconditions.checkState(searchInProgress,
                "Attempted to fetch response for non-active search query on [%s.%s]", schema, table);

        SearchResponse response = future.actionGet();
        searchInProgress = false;

        totalProcessedCount += response.getHits().getHits().length;
        totalHitCount = response.getHits().getTotalHits();

        logger.info("Search on [{}.{}] returned [{}] hits in {} ms.", schema, table,
                response.getHits().getHits().length, response.getTookInMillis());

        return response;
    }

    public long totalHitCount() {
        return totalHitCount;
    }

    public boolean finished() {

        if (searchInProgress) {
            return false;
        }

        return (totalProcessedCount >= totalHitCount);
    }
}
