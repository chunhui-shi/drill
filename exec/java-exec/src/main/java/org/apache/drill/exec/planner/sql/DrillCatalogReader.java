/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaImpl;
import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.store.SchemaConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link org.apache.calcite.prepare.Prepare.CatalogReader}
 * and also {@link org.apache.calcite.sql.SqlOperatorTable} based on tables and
 * functions defined schemas.
 */
public class DrillCatalogReader extends CalciteCatalogReader {

  final private QueryContext queryContext;

  public DrillCatalogReader(
      QueryContext qcontext,
      CalciteSchema rootSchema,
      boolean caseSensitive,
      List<String> defaultSchema,
      JavaTypeFactory typeFactory) {
    super(rootSchema, caseSensitive, defaultSchema, typeFactory);
    assert rootSchema != defaultSchema;
    queryContext = qcontext;
  }

  public DrillCatalogReader withSchemaPath(List<String> schemaPath) {
    return new DrillCatalogReader(queryContext, super.getSchema(ImmutableList.<String>of()),
        this.isCaseSensitive(), schemaPath, (JavaTypeFactory)getTypeFactory());
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public CalciteSchema getSchema(Iterable<String> schemaNames) {

    //get 'rootSchema'
    CalciteSchema existingRootSchema = super.getSchema(ImmutableList.<String>of());
    CalciteSchema schema = existingRootSchema;
    int layer = 0;
    for (String schemaName : schemaNames) {
      schema = schema.getSubSchema(schemaName, isCaseSensitive());
      if (schema == null) {
        if (layer == 0) {
          final Set<String> strSet = Sets.newHashSet();
          strSet.add(schemaName);
          if(schemaName.contains(".")) {
            String[] schemaArray = schemaName.split("\\.");
            String prefix = schemaArray[0];
            for(int i=1; i<schemaArray.length; ++i) {
              strSet.add(prefix);
              prefix = Joiner.on(".").join(prefix, schemaArray[i]);
            }
          }

          //queryContext.addNewRelevantSchema(strSet, existingRootSchema.plus());
          SchemaPlus rootSchema = existingRootSchema.plus();
          queryContext.getSchemaTreeProvider().addPartialRootSchema(queryContext.getQueryUserName(),
              queryContext, strSet, rootSchema);
          SchemaPlus plus = rootSchema.getSubSchema(schemaName);
          if (plus != null) {
            schema = SimpleCalciteSchema.from(plus);
          }
        }
      }
      if(schema == null) {
        return null;
      }
      layer++;
    }
    return schema;
  }


}


