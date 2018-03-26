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
package org.apache.drill.exec.physical.impl.lateraljoin;

import static org.junit.Assert.assertEquals;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Ignore;
import org.junit.Test;

public class TestLateralPhysicalPlan extends BaseTestQuery {

  @Test
  public void testLateralPlan1() throws Exception {
    int numOutputRecords = testPhysical(getFile("lateraljoin/lateralplan1.json"));
    assertEquals(numOutputRecords, 12);
  }

  @Test
  @Ignore("To be fixed")
  public void testLateralSqlStar() throws Exception {
    String Sql = "select * from cp.`lateraljoin/nested-customer.json` t, unnest(t.orders) t2 limit 1";
    testBuilder()
        .unOrdered()
        .sqlQuery(Sql)
        .baselineColumns("c_name", "o_shop")
        .baselineValues("customer1", "Meno Park 1st")
        .go();

  }

  @Test
  public void testLateralSql() throws Exception {
    String Sql = "select t.c_name, t2.o_shop as o_shop from cp.`lateraljoin/nested-customer.json` t, unnest(t.orders) t2 limit 1";
    testBuilder()
        .unOrdered()
        .sqlQuery(Sql)
        .baselineColumns("c_name", "o_shop")
        .baselineValues("customer1", "Meno Park 1st")
        .go();

  }

  @Test
  @Ignore("naming of single column")
  public void testLateralSqlPlainCol() throws Exception {
    String Sql = "select t.c_name, t2.c_phone from cp.`lateraljoin/nested-customer.json` t, unnest(t.c_phone) t2 limit 1";
    testBuilder()
        .unOrdered()
        .sqlQuery(Sql)
        .baselineColumns("c_name", "c_phone_flat")
        .baselineValues("customer1", "6505200001")
        .go();

  }

  @Test
  @Ignore("To be fixed")
  public void testLateralSqlWithAS() throws Exception {
    String Sql = "select t.c_name, t2.o_shop from cp.`lateraljoin/nested-customer.parquet` t, unnest(t.orders) as t2(o_shop) limit 1";
    testBuilder()
        .unOrdered()
        .sqlQuery(Sql)
        .baselineColumns("c_name", "o_shop")
        .baselineValues("customer1", "Meno Park 1st")
        .go();

  }
  @Test
  public void testSubQuerySql() throws Exception {
    String Sql = "select t2.os.* from (select t.orders as os from cp.`lateraljoin/nested-customer.parquet` t) t2";
    test(Sql);
  }
}
