package federated.sql.schema;

import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.shardingsphere.infra.config.datasource.DataSourceParameter;
import org.apache.shardingsphere.infra.datanode.DataNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static federated.sql.metadata.LogicSchemaConstants.MYSQL_DRIVER;

/**
 * Logic schema.
 *
 */
@Getter
public final class LogicSchema extends AbstractSchema {
    
    private final Map<String, DataSourceParameter> dataSourceParameters = new LinkedHashMap<>();

    private static final String MYSQL_GET_SCHEMA_SENTENCE = "select table_name from information_schema.tables where table_type = ? and TABLE_SCHEMA = ?";
    private static final String NORMAL_SCHEMA_TYPE = "BASE TABLE";

    private Map<String, Table> tableMap;
    
    public LogicSchema(final Map<String, DataSourceParameter> dataSourceParameters, final Map<String, Collection<DataNode>> dataNodes) {
        
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
