package federated.sql.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;

/**
 * Logic Table.
 *
 */
public class LogicScannableTable extends AbstractLogicTable implements ScannableTable {
    
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }
}
