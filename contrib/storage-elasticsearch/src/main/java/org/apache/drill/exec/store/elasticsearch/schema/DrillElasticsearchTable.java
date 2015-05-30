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
package org.apache.drill.exec.store.elasticsearch.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchScanSpec;
import org.apache.drill.exec.store.elasticsearch.types.ElasticsearchType;

import java.util.ArrayList;
import java.util.List;

/**
 * Table based on an elasticsearch index.
 * <p>
 * Terminology changes to reflect the relational world:
 * </p>
 * <p><ul>
 *  <li>A relational <b>schema</b> is equivalent to an elasticsearch <b>index</b>.</li>
 *  <li>A relational <b>table</b> is equivalent to an elasticsearch <b>document type</b> (i.e. a type within an index, not a data type).</li>
 *  <li>A relational <b>field (or column)</b> is equivalent to an elasticsearch <b>field</b>.</li>
 * </ul></p>
 */
public class DrillElasticsearchTable extends DrillTable {

    private final TableDefinition table;

    public DrillElasticsearchTable(TableDefinition table, String storageEngineName, StoragePlugin plugin,
                                   ElasticsearchScanSpec selection) {
        super(storageEngineName, plugin, selection);
        this.table = table;
    }

    /**
     * Converts this table definition into the appropriate relational data type.
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        final List<String>      names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();

        // XXX - Fix this to work with nested types!!!

        for (ColumnDefinition column : table.columns()) {
            names.add(column.name());
            types.add(column.type().to(typeFactory));
        }

        return typeFactory.createStructType(types, names);
    }

    /**
     * Schema in which this table resides.
     */
    public String schema() {
        return table.schema;
    }

    /**
     * Name of the table.
     */
    public String name() {
        return table.name;
    }

    /**
     * Whether or not the '_source' flag has been set on the
     * elasticsearch index backing this table.
     */
    public boolean source() {
        return table.source;
    }

    /**
     * A list of this table's columns.
     */
    public List<ColumnDefinition> columns() {
        return table.columns;
    }

    @Override
    public String toString() {
        return "DrillElasticsearchTable{" +
                "table=" + table +
                '}';
    }

    /**
     * A table definition.
     */
    public static class TableDefinition {

        private final String  schema;
        private final String  name;
        private final boolean source;

        private final List<ColumnDefinition> columns;

        public TableDefinition(String schema, String name, boolean source, List<ColumnDefinition> columns) {
            this.schema  = schema;
            this.name    = name;
            this.source  = source;
            this.columns = columns;
        }

        public String schema() {
            return schema;
        }

        public String name() {
            return name;
        }

        public List<ColumnDefinition> columns() {
            return columns;
        }

        public ColumnDefinition select(SchemaPath path) {
            return select(path, columns);
        }

        public ColumnDefinition select(SchemaPath path, List<ColumnDefinition> cols) {

            for (ColumnDefinition col : cols) {
                if (path.equals(col.path)) {
                    col.selected = true;
                    return col;
                }
                else if (col.hasChildren()) {
                    ColumnDefinition cd = select(path, col.children());
                    if (cd != null) {
                        col.selected = true;
                        return cd;
                    }
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "TableDefinition{" +
                    "schema='" + schema + '\'' +
                    ", name='" + name + '\'' +
                    ", source=" + source +
                    ", columns=" + columns +
                    '}';
        }
    }

    /**
     * A column definition.
     */
    public static class ColumnDefinition {

        private final String            name;
        private final SchemaPath        path;
        private final ElasticsearchType type;

        private final List<ColumnDefinition> children = new ArrayList<>();

        private boolean selected = false;

        public ColumnDefinition(String name, SchemaPath path, ElasticsearchType type) {
            this.name = name;
            this.path = path;
            this.type = type;
        }

        public SchemaPath path() {
            return path;
        }

        public ElasticsearchType type() {
            return type;
        }

        public String name() {
            return name;
        }

        public boolean selected() {
            return selected;
        }

        public boolean hasChildren() {
            return children.size() > 0;
        }

        public List<ColumnDefinition> children() {
            return children;
        }

        public void addChild(ColumnDefinition columnDefinition) {
            children.add(columnDefinition);
        }

        @Override
        public String toString() {
            return "ColumnDefinition{" +
                    "path=" + path +
                    ", type=" + type +
                    ", selected=" + selected +
                    ", children=" + children +
                    '}';
        }
    }
}
