package federated.sql.schema;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import federated.sql.metadata.DataSourceParameter;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.shardingsphere.infra.datanode.DataNode;

import javax.sql.DataSource;
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
    
    final Map<String, Table> tables = new LinkedMap<>();

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
    
    private Table createTable() {
    
    }
    
    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }
}
