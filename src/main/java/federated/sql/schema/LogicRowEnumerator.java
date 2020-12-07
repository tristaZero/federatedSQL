package federated.sql.schema;

import org.apache.calcite.linq4j.Enumerator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Logic row enumerator.
 *
 * @author panjuan
 * @author zhaojun
 */
public final class LogicRowEnumerator implements Enumerator<Object[]> {
    
    private final Collection<ResultSet> resultSets = new LinkedList<>();
    
    private final Iterator<ResultSet> iterator;
    
    private ResultSet currentResultSet;
    
    private Object[] currentRow;
    
    public LogicRowEnumerator(final Collection<ResultSet> resultSets) {
        this.resultSets.addAll(resultSets);
        iterator = this.resultSets.iterator();
        currentResultSet = iterator.next();
    }
    
    @Override
    public Object[] current() {
        return currentRow;
    }
    
    @Override
    public boolean moveNext() {
        try {
            return moveNext0();
        } catch (final SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private boolean moveNext0() throws SQLException {
        if (currentResultSet.next()) {
            setCurrentRow();
            return true;
        }
        if (!iterator.hasNext()) {
            currentRow = null;
            return false;
        }
        currentResultSet = iterator.next();
        if (currentResultSet.next()) {
            setCurrentRow();
            return true;
        }
        return false;
    }
    
    private void setCurrentRow() throws SQLException {
        int columnCount = currentResultSet.getMetaData().getColumnCount();
        currentRow = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            currentRow[i] = currentResultSet.getObject(i+1);
        }
    }
    @Override
    public void reset() {
    }
    
    @Override
    public void close() {
        try {
            for (ResultSet each : resultSets) {
                each.getStatement().getConnection().close();
                each.getStatement().close();
                each.close();
            }
            currentRow = null;
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
