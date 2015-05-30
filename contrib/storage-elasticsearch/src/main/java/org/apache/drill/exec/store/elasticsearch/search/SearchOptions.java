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

import com.google.common.base.Preconditions;

import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * Options to control how queries against elasticsearch are executed.
 */
public class SearchOptions {

    private static final TimeValue DEFAULT_KEEP_ALIVE = new TimeValue(1, TimeUnit.MINUTES);

    private int       batchSize;
    private TimeValue scrollTimeout = DEFAULT_KEEP_ALIVE;

    public SearchOptions(int batchSize, long scrollTimeout) {

        Preconditions.checkArgument(batchSize > 1, "Batch size must be greater than 1");
        Preconditions.checkArgument(scrollTimeout > 1, "Scroll timeout must be greater than 1");

        this.batchSize = batchSize;
        this.scrollTimeout = TimeValue.timeValueMillis(scrollTimeout);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public TimeValue getScrollTimeout() {
        return scrollTimeout;
    }

    public void setScrollTimeout(long millis) {
        this.scrollTimeout = TimeValue.timeValueMillis(millis);
    }
}
