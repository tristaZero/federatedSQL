package federated.sql.executor;

import federated.sql.schema.LogicSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public final class CalciteRawExecutorTest {
    
    private final Properties properties = new Properties();
    
    private final String testSQL = "SELECT order_id FROM t_order where order_id < 10";
    
    private final String joinSQL = "SELECT * FROM t_order, t_order_item where t_order.order_id = t_order_item.order_id";
    
    /**
     * t_order and t_order_item are sharding tables. Here is the distribution fo them,
     *
     *    ds0                  ds1
     * t_order_0            t_order_1
     * t_order_item_0       t_order_item_1
     *
     * That also means t_oder (logic table) is made of `ds0.t_order_0` and `ds1.t_order_1` (actual tables).
     * Similarly, `ds0.t_order_item_0` and `ds1.t_order_item_1` makes up t_order_item
     * But for user, they just know there are two logic tables, i.e., t_order and t_order_item in `sharding` database.
     */
    @Before
    public void setUp() {
        properties.setProperty("lex", Lex.MYSQL.name());
        properties.setProperty("conformance", SqlConformanceEnum.MYSQL_5.name());
        properties.setProperty("schemaFactory", LogicSchemaFactory.class.getCanonicalName());
        properties.setProperty("schema", "sharding");
        properties.setProperty("schema.dataSources.ds0.url", "jdbc:mysql://127.0.0.1:3306/demo_ds_0");
        properties.setProperty("schema.dataSources.ds0.username", "root");
        properties.setProperty("schema.dataSources.ds0.password", "");
        properties.setProperty("schema.dataSources.ds0.driver", "com.mysql.jdbc.Driver");
        properties.setProperty("schema.dataSources.ds1.url", "jdbc:mysql://127.0.0.1:3306/demo_ds_1");
        properties.setProperty("schema.dataSources.ds1.username", "root");
        properties.setProperty("schema.dataSources.ds1.password", "");
        properties.setProperty("schema.dataSources.ds1.driver", "com.mysql.jdbc.Driver");
        properties.setProperty("schema.tableRules.t_order.dataNodes", "ds0.t_order_0,ds1.t_order_1");
        properties.setProperty("schema.tableRules.t_order_item.dataNodes", "ds0.t_order_item_0,ds1.t_order_item_1");
    }
    
    @Test
    public void assertSingleExecute() throws SqlParseException {
        CalciteRawExecutor executor = new CalciteRawExecutor((properties));
        Enumerable<Object[]> result = executor.execute(testSQL);
        int rowCount = 0;
        for (Object[] each : result) {
            rowCount +=1;
            for (Object o : each) {
                System.out.println(o);
            }
        }
        assertThat(rowCount, is(2));
    }
    
    @Test
    public void assertJoinExecute() throws SqlParseException {
        CalciteRawExecutor executor = new CalciteRawExecutor((properties));
        Enumerable<Object[]> result = executor.execute(joinSQL);
        int rowCount = 0;
        for (Object[] each : result) {
            rowCount +=1;
            for (Object o : each) {
                System.out.println(o);
            }
        }
        assertThat(rowCount, is(1));
    }
}
