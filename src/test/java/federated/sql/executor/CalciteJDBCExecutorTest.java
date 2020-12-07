package federated.sql.executor;

import federated.sql.schema.LogicSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public final class CalciteJDBCExecutorTest {
    
    private final Properties properties = new Properties();
    
    private final String testSQL = "SELECT order_id FROM t_order where order_id < 10";
    
    private final String joinSQL = "SELECT * FROM t_order, t_order_item where t_order.order_id = t_order_item.order_id";
    
    @Before
    public void setUp() {
        properties.setProperty("lex", Lex.MYSQL.name());
        properties.setProperty("conformance", SqlConformanceEnum.MYSQL_5.name());
        properties.setProperty("schemaFactory", LogicSchemaFactory.class.getCanonicalName());
        properties.setProperty("schema", "sharding");
        properties.setProperty("schema.dataSources.ds0.url", "jdbc:mysql://127.0.0.1:3306/demo_ds_0");
        properties.setProperty("schema.dataSources.ds0.username", "root");
        properties.setProperty("schema.dataSources.ds0.password", "");
        properties.setProperty("schema.dataSources.ds0.driver", "com.mysql.jdbc.Driver");
        properties.setProperty("schema.dataSources.ds1.url", "jdbc:mysql://127.0.0.1:3306/demo_ds_1");
        properties.setProperty("schema.dataSources.ds1.username", "root");
        properties.setProperty("schema.dataSources.ds1.password", "");
        properties.setProperty("schema.dataSources.ds1.driver", "com.mysql.jdbc.Driver");
        properties.setProperty("schema.tableRules.t_order.dataNodes", "ds0.t_order_0,ds1.t_order_1");
        properties.setProperty("schema.tableRules.t_order_item.dataNodes", "ds0.t_order_item_0,ds1.t_order_item_1");
    }
    
    @Test
    public void assertSingleExecute() {
        CalciteJDBCExecutor executor = new CalciteJDBCExecutor(properties);
        try (ResultSet resultSet = executor.execute(testSQL, Collections.emptyList())) {
            assertSingleResultSet(resultSet);
            executor.clearResultSet();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private void assertSingleResultSet(final ResultSet resultSet) throws SQLException {
        int rowCount = 0;
        while (resultSet.next()) {
            rowCount +=1;
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                System.out.println(resultSet.getMetaData().getColumnName(i) + ": " + resultSet.getString(i));
            }
        }
        assertThat(rowCount, is(2));
    }
    
    @Test
    public void assertJoinExecute() {
        CalciteJDBCExecutor executor = new CalciteJDBCExecutor(properties);
        try (ResultSet resultSet = executor.execute(joinSQL, Collections.emptyList())) {
            assertJoinResultSet(resultSet);
            executor.clearResultSet();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private void assertJoinResultSet(final ResultSet resultSet) throws SQLException {
        int rowCount = 0;
        while (resultSet.next()) {
            rowCount +=1;
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                System.out.println(resultSet.getMetaData().getColumnName(i) + ": " + resultSet.getString(i));
            }
        }
        assertThat(rowCount, is(2));
    }
}
