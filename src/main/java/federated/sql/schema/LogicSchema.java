package federated.sql.schema;

import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import federated.sql.metadata.DataSourceParameter;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.shardingsphere.infra.datanode.DataNode;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Logic schema.
 *
 */
@Getter
public final class LogicSchema extends AbstractSchema {
    
    private final Map<String, DataSource> dataSources = new LinkedHashMap<>();
    
    final Map<String, Collection<LogicTable>> tables = new LinkedMap<>();

    private static final String MYSQL_GET_SCHEMA_SENTENCE = "select table_name from information_schema.tables where table_type = ? and TABLE_SCHEMA = ?";
    private static final String NORMAL_SCHEMA_TYPE = "BASE TABLE";

    private Map<String, Table> tableMap;
    
    public LogicSchema(final Map<String, DataSourceParameter> dataSourceParameters, final Map<String, Collection<DataNode>> dataNodes) {
        for (Map.Entry<String, DataSourceParameter> entry : dataSourceParameters.entrySet()) {
            dataSources.put(entry.getKey(), createDataSource(entry.getValue()));
        }
        
    }
    
    private DataSource createDataSource(final DataSourceParameter dataSourceParameter) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(dataSourceParameter.getDiver());
        config.setJdbcUrl(dataSourceParameter.getUrl());
        config.setUsername(dataSourceParameter.getUsername());
        config.setPassword(dataSourceParameter.getPassword());
        return new HikariDataSource(config);
    }
    
    
    
    @Override
    protected Map<String, Table> getTableMap() {
        if (!tableMap.isEmpty()) {
            return tableMap;
        }
        Connection connection;
        try {
            Class.forName(MYSQL_DRIVER);
            //should need
            connection = DriverManager.getConnection(url, username, password);
            PreparedStatement preparedStatement = connection.prepareStatement(MYSQL_GET_SCHEMA_SENTENCE);

            preparedStatement.setString(1, NORMAL_SCHEMA_TYPE);
            preparedStatement.setString(2, schema);

            ResultSet resultSet = preparedStatement.executeQuery();

            tableMap = Maps.newHashMap();
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                Table mysqlTable = new LogicTable(schema, tableName, connection);
                tableMap.put(tableName, mysqlTable);
            }
            return tableMap;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
