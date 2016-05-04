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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.DrillBuf;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.elasticsearch.schema.SchemaProvider;
import org.apache.drill.exec.store.elasticsearch.search.SearchOptions;
import org.apache.drill.exec.store.elasticsearch.search.Searcher;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable.TableDefinition;
import org.apache.drill.exec.store.elasticsearch.schema.DrillElasticsearchTable.ColumnDefinition;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.search.SearchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;

import java.util.Queue;

import static java.lang.String.format;


/**
 * Record reader for elasticsearch.
 */
public class ElasticsearchRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordReader.class);

    private final String schema;
    private final String table;
    private final Client client;
    private final String queryString;
    private final FragmentContext context;

    private long totalProcessedCount = 0;

    private Searcher              searcher;
    private TableDefinition       definition;
    private VectorContainerWriter container;
    private MapWriter             writer;
    private SearchOptions         options;

    public ElasticsearchRecordReader(FragmentContext context, String schema, String table, String queryString,
                                     SearchOptions options, List<SchemaPath> columns, Client client)
        throws IOException, UnsupportedTypeException {

        this.context     = Preconditions.checkNotNull(context);
        this.schema      = Preconditions.checkNotNull(schema);
        this.table       = Preconditions.checkNotNull(table);
        this.client      = Preconditions.checkNotNull(client);
        this.options     = Preconditions.checkNotNull(options);
        this.queryString = queryString;
        this.definition  = new SchemaProvider(client).table(schema, table);

        setColumns(columns);
    }

    private void getAllColumns(ImmutableList.Builder<SchemaPath> builder,
                               List<ColumnDefinition> toGoThroughColumns,
                               Collection<SchemaPath> columns) {
        Queue<ColumnDefinition> queue = new LinkedList<>(toGoThroughColumns);
        while ( !queue.isEmpty() ) {
            ColumnDefinition col = queue.remove();
            if(col.hasChildren()) {
                queue.addAll(col.children());
            }
            else {
                if (columns != null) {
                    for (SchemaPath path : columns) {
                        if ( col.path().contains(path)) {
                            builder.add(col.path());
                            break;
                        }
                    }
                }
                else {
                    builder.add(col.path());
                }
            }
        }
    }
    @Override
    protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {

        ImmutableList.Builder<SchemaPath> builder = ImmutableList.builder();
        if (!isStarQuery()) {
            getAllColumns(builder, definition.columns(), columns);
        }
        else {
            // Expand '*' into all columns
            getAllColumns(builder, definition.columns(), null);
        }
        return builder.build();
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {

        logger.debug("Configuring reader for: [{}.{}:{}]", schema, table, getColumns());

        for (SchemaPath path : getColumns()) {
            if (definition.select(path) == null) {
                throw new ExecutionSetupException(format("Column [%s] does not exist in table: [%s.%s]",
                        path, schema, table));
            }
        }

        container = new VectorContainerWriter(output);
        writer = container.rootAsMap();

        // Start the query against elasticsearch asynchronously
        searcher = new Searcher(client, schema, table, getColumns(), queryString, options);
        searcher.search();
    }

    @Override
    public int next() {

        Preconditions.checkNotNull(searcher);
        if (searcher.finished()) {
            return 0;
        }

        // We may block here pending a response
        SearchResponse response = searcher.response();

        container.allocate();
        container.reset();

        int n = process(response);
        totalProcessedCount += n;
        logger.info("Processed [{}/{}] records", totalProcessedCount, searcher.totalHitCount());
        container.setValueCount(n);

        searcher.search(response.getScrollId());

        return n;
    }

    private int process(SearchResponse response) {

        int processed = 0;
        for (SearchHit hit : response.getHits().getHits()) {

            writer.setPosition(processed);
            processed++;

            if (hit.isSourceEmpty()) {
                continue;
            }

            Map<String, Object> source = hit.sourceAsMap();


            for (ColumnDefinition column : definition.columns()) {
                if (column.selected()) {
                    if(column.name() == ElasticsearchConstants.ROW_KEY) {
                        populate(column, hit.id(), writer);
                    }
                    else {
                        populate(column, source.get(column.name()), writer);
                    }
                }
            }
        }
        return processed;
    }

    private void populate(ColumnDefinition column, Object value, MapWriter writer) {

        if (value == null) {
            return;
        }

        // XXX - Skip arrays for now.
        boolean isArray = false;
        if (value instanceof List) {
            isArray = true;
            return;
        }

        String name = column.name();

        switch (column.type()) {
            case INTEGER:
                writer.integer(name).writeInt(((Number) value).intValue());
                break;
            case LONG:
                writer.bigInt(name).writeBigInt(((Number) value).longValue());
                break;
            case FLOAT:
                writer.float4(name).writeFloat4(((Number) value).floatValue());
                break;
            case DOUBLE:
                writer.float8(name).writeFloat8(((Number) value).doubleValue());
                break;
            case BOOLEAN:
                int b = ((Boolean) value) ? 1 : 0;
                writer.bit(name).writeBit(b);
                break;
            case STRING:
                DrillBuf buf = context.getManagedBuffer();
                byte[] bytes = ((String) value).getBytes(Charsets.UTF_8);
                buf.reallocIfNeeded(bytes.length);
                buf.setBytes(0, bytes);
                writer.varChar(name).writeVarChar(0, bytes.length, buf);
                break;
            case NESTED:

                @SuppressWarnings("unchecked")
                Map<String, Object> nestedValueMap = (Map) value;
                MapWriter subWriter = writer.map(name);

                for (ColumnDefinition col : column.children()) {
                    if (col.selected()) {
                        String nestedFieldName = col.path().getLastSegment().getNameSegment().getPath();
                        populate(col, nestedValueMap.get(nestedFieldName), subWriter);
                    }
                }
                break;
            default:
                logger.warn(format("Unsupported column type: [%s] for field: [%s]", column.type(), name));
                break;
        }
    }

    @Override
    public void close() {
        logger.debug("Cleaning up reader for: [{}.{}:{}]", schema, table, getColumns());
    }

    @Override
    public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
        super.allocate(vectorMap);
    }
}
