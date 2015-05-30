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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchSubScan.ElasticsearchSubScanSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ElasticsearchScanBatchCreator implements BatchCreator<ElasticsearchSubScan> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchScanBatchCreator.class);

    @Override
    public ScanBatch getBatch(FragmentContext context, ElasticsearchSubScan subScan,
                              List<RecordBatch> children) throws ExecutionSetupException {

        Preconditions.checkArgument(children.isEmpty());
        List<RecordReader> readers = new ArrayList<>();

        List<ElasticsearchSubScanSpec> subs = subScan.getSubScanSpecs();
        // XXX - For now we expect exactly 1 sub-scan. This will change when/if we do node affinity.
        assert subs.size() == 1;
        ElasticsearchSubScanSpec sub = subs.get(0);

        try {
            readers.add(new ElasticsearchRecordReader(context, sub.getSchema(), sub.getTable(), sub.getQuery(),
                    sub.getOptions(), subScan.getColumns(), subScan.getPlugin().getConfig().getClient()));
        }
        catch (UnsupportedTypeException | IOException e) {
            logger.error("Failed to create elasticsearch record reader: [{}]", e.getMessage(), e);
            throw new ExecutionSetupException(e.getMessage(), e.getCause());
        }

        return new ScanBatch(subScan, context, readers.iterator());
    }
}
