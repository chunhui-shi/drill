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
package org.apache.drill.exec.store.elasticsearch.types;

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;
import java.util.Map;

/**
 * Elasticsearch data types.
 * <p>
 * @see "https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-core-types.html"
 * </p>
 */
public enum ElasticsearchType {

    INTEGER   (SqlTypeName.INTEGER),
    LONG      (SqlTypeName.BIGINT),
    FLOAT     (SqlTypeName.FLOAT),
    DOUBLE    (SqlTypeName.DOUBLE),
    BOOLEAN   (SqlTypeName.BOOLEAN),
    STRING    (SqlTypeName.VARCHAR),

    NESTED(SqlTypeName.MAP, false) {
        @Override
        public RelDataType to(RelDataTypeFactory typeFactory) {
            return typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY));
        }
    },
    OBJECT(SqlTypeName.MAP, false) {
        @Override
        public RelDataType to(RelDataTypeFactory typeFactory) {
            return typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY));
        }
    },
    GEO_POINT(SqlTypeName.MAP, false) {
        @Override
        public RelDataType to(RelDataTypeFactory typeFactory) {
            return typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.DOUBLE));
        }
    },
    GEO_SHAPE(SqlTypeName.ARRAY, false) {
        @Override
        public RelDataType to(RelDataTypeFactory typeFactory) {
            return typeFactory.createArrayType(GEO_POINT.to(typeFactory), -1);
        }
    };

    ElasticsearchType(SqlTypeName sqlTypeName) {
        this(sqlTypeName, true);
    }

    ElasticsearchType(SqlTypeName sqlTypeName, boolean primitive) {
        this.sqlTypeName = sqlTypeName;
        this.primitive   = primitive;
    }

    public static final Map<String, ElasticsearchType> types;

    static {
        ImmutableMap.Builder<String, ElasticsearchType> builder = ImmutableMap.builder();
        for (ElasticsearchType type : values()) {
            builder.put(type.name().toLowerCase(Locale.ENGLISH), type);
        }
        types = builder.build();
    }

    final SqlTypeName sqlTypeName;
    final boolean     primitive;

    /**
     * Gets the appropriate calcite SQL type from the provided type factory.
     */
    public RelDataType to(RelDataTypeFactory typeFactory) {
        return typeFactory.createSqlType(sqlTypeName);
    }

    /**
     * Returns the corresponding elasticsearch type of the given name.
     */
    public static ElasticsearchType of(String type) {
        return types.get(type);
    }

    @Override
    public String toString() {
        return "ElasticTypeEnum{" +
                "sqlTypeName=" + sqlTypeName +
                ", primitive=" + primitive +
                '}';
    }
}
