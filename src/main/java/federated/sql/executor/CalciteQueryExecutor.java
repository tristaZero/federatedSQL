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

package federated.sql.executor;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Calcite query executor.
 */
public final class CalciteQueryExecutor {
    
    private static final String DRIVER_NAME = "org.apache.calcite.jdbc.Driver";
    
    private static final String CONNECTION_URL = "jdbc:calcite:";
    
    private final Properties properties;
    
    private Statement statement;
    
    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (final ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public CalciteQueryExecutor(final Properties connectionProps) {
        properties = connectionProps;
    }
    
    /**
     * Execute.
     *
     * @return calcite query result
     * @throws SQLException SQL exception
     */
    public ResultSet execute(final String sql, final List<Object> parameters) throws SQLException {
        PreparedStatement statement = DriverManager.getConnection(CONNECTION_URL, properties).prepareStatement(sql);
        setParameters(statement, parameters);
        return statement.executeQuery();
    }
    
    private void setParameters(final PreparedStatement preparedStatement, final List<Object> parameters) throws SQLException {
        int count = 1;
        for (Object each : parameters) {
            preparedStatement.setObject(count, each);
            count++;
        }
    }
    
    /**
     * Clear resultSet.
     *
     * @throws Exception exception
     */
    public void clearResultSet() throws Exception {
        statement.getConnection().close();
        statement.close();
    }
}
