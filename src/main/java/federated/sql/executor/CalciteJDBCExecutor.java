package federated.sql.executor;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static federated.sql.metadata.LogicSchemaConstants.CONNECTION_URL;
import static federated.sql.metadata.LogicSchemaConstants.DRIVER_NAME;

/**
 * Calcite jdbc executor.
 *
 * This executor used Calcite JDBC driver to execute SQL.
 */
public final class CalciteJDBCExecutor {
    
    private final Properties properties;
    
    private Statement statement;
    
    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (final ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public CalciteJDBCExecutor(final Properties connectionProps) {
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
        this.statement = statement;
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
