package federated.sql.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/**
 * Logic Table.
 *
 */
public class LogicScannableTable extends AbstractLogicTable implements ScannableTable {
    
    public LogicScannableTable(Map<String, DataSource> dataSources, Collection<DataNode> dataNodes, DatabaseType databaseType) throws SQLException {
        super(dataSources, dataNodes, databaseType);
        
    }
    
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }
}
