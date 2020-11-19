package federated.sql.schema;

import com.google.common.base.Joiner;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Logic Table.
 *
 */
public final class LogicScannableTable extends AbstractLogicTable implements ScannableTable {
    
    private final Map<DataNode, String> tableSQLs = new LinkedMap<>();
    
    public LogicScannableTable(Map<String, DataSource> dataSources, Collection<DataNode> dataNodes, DatabaseType databaseType) throws SQLException {
        super(dataSources, dataNodes, databaseType);
        String columns = Joiner.on(",").join(getTableMetaData().getColumns().keySet());
        for (DataNode each : dataNodes) {
            tableSQLs.put(each, String.format("SELECT %s FROM %s", columns, each.getTableName()));
        }
    }
    
    @Override
    public Enumerable<Object[]> scan(final DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            
            @Override
            public Enumerator<Object[]> enumerator() {
                return new LogicRowEnumerator(getResultSets());
            }
        };
    }
    
    private Collection<ResultSet> getResultSets() {
        Collection<ResultSet> resultSets = new LinkedList<>();
        for (Entry<DataNode, String> entry : tableSQLs.entrySet()) {
        resultSets.add(getResultSet(entry));
        }
        return resultSets;
    }
    
    private ResultSet getResultSet(final Entry<DataNode, String> tableSQL) {
        try {
            Statement statement = getDataSources().get(tableSQL.getKey().getDataSourceName()).getConnection().createStatement();
            return statement.executeQuery(tableSQL.getValue());
        } catch (final SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
