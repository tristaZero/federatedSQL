package federated.sql.executor;

import federated.sql.schema.LogicSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

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
public final class CalciteQueryExecutorTest {
    
    private final Properties properties = new Properties();
    
    private final String testSQL = "SELECT * FROM t_order where order_id < 10";
    
    @Before
    public void setUp() throws Exception {
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
    public void assertExecute() {
        CalciteQueryExecutor executor = new CalciteQueryExecutor(properties);
        try (ResultSet resultSet = executor.execute(testSQL, Collections.emptyList())) {
            System.out.println(getResultSetStr(resultSet));
            executor.clearResultSet();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private String getResultSetStr(final ResultSet resultSet) throws SQLException {
        StringBuilder builder = new StringBuilder();
        while (resultSet.next()) {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                builder.append(resultSet.getMetaData().getColumnName(i)).append(": ").append(resultSet.getString(i));
            }
        }
        return builder.toString();
    }
}
