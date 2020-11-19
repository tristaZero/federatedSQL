package federated.sql.schema;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import federated.sql.metadata.DataSourceParameter;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.datanode.DataNode;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Logic schema.
 *
 */
@Getter
public final class LogicSchema extends AbstractSchema {
    
    final Map<String, Table> tables = new LinkedMap<>();

    public LogicSchema(final Map<String, DataSourceParameter> dataSourceParameters, final Map<String, Collection<DataNode>> dataNodes) throws SQLException {
        DatabaseType databaseType = DatabaseTypeRegistry.getDatabaseTypeByURL(dataSourceParameters.values().iterator().next().getUrl());
        Map<String, DataSource> dataSources = createDataSources(dataSourceParameters);
        for (Entry<String, Collection<DataNode>> entry : dataNodes.entrySet()) {
            tables.put(entry.getKey(), createTable(dataSources, entry.getValue(), databaseType));
        }
    }
    
    private Map<String, DataSource> createDataSources(final Map<String, DataSourceParameter> dataSourceParameters) {
        Map<String, DataSource> result = new LinkedHashMap<>();
        for (Entry<String, DataSourceParameter> entry : dataSourceParameters.entrySet()) {
            result.put(entry.getKey(), createDataSource(entry.getValue()));
        }
        return result;
    }
    
    private DataSource createDataSource(final DataSourceParameter dataSourceParameter) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(dataSourceParameter.getDiver());
        config.setJdbcUrl(dataSourceParameter.getUrl());
        config.setUsername(dataSourceParameter.getUsername());
        config.setPassword(dataSourceParameter.getPassword());
        return new HikariDataSource(config);
    }
    
    private Table createTable(final Map<String, DataSource> dataSources, final Collection<DataNode> dataNodes, final DatabaseType databaseType) throws SQLException {
        Map<String, DataSource> tableDataSources = new LinkedMap<>();
        for (DataNode each : dataNodes) {
            if (dataSources.containsKey(each.getDataSourceName())) {
                tableDataSources.put(each.getDataSourceName(), dataSources.get(each.getDataSourceName()));
            }
        }
        return new LogicScannableTable(tableDataSources, dataNodes, databaseType);
    }
    
    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }
}
