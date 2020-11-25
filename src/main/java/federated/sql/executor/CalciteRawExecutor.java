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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Calcite raw executor.
 */
public final class CalciteRawExecutor {
    
    private final Properties properties;
    
    private final CalciteConnectionConfig config;
    
    private final RelDataTypeFactory typeFactory;
    
    private final SchemaFactory factory;
    
    private final CalciteSchema schema;
    
    public CalciteRawExecutor(final Properties connectionProps) {
        properties = connectionProps;
        config = new CalciteConnectionConfigImpl(properties);
        typeFactory = new JavaTypeFactoryImpl();
        factory = config.schemaFactory(SchemaFactory.class, null);
        schema = CalciteSchema.createRootSchema(true);
        schema.add(config.schema(), new ReflectiveSchema(factory.create(null, config.schema(), getOperands())));
    }
    
    private Map<String, Object> getOperands() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String each : properties.stringPropertyNames()) {
            if (each.startsWith("schema.")) {
                result.put(each.substring("schema.".length()), properties.getProperty(each));
            }
        }
        return result;
    }
    
    /**
     * Execute.
     *
     * @return calcite query result
     * @throws SQLException SQL exception
     */
    public ResultSet execute(final String sql, final List<Object> parameters) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema, Collections.singletonList(config.schema()), typeFactory, config);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
        SqlNode validNode = validator.validate(sqlNode);
        
        RelOptCluster cluster = newCluster(typeFactory);
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.Config.DEFAULT);
    
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;
        
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
