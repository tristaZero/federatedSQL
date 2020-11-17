package federated.sql.schema;

import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Logic Table.
 *
 */
public class LogicTable extends AbstractTable implements TranslatableTable {

    private static final String MYSQL_COLUMN_META_SQL =
            "select COLUMN_NAME, DATA_TYPE from information_schema.columns where table_schema = ? and table_name = ?";

    private LogicRowsReaderImpl mysqlReader;
    private RelToSqlConverter relToSqlConverter;
    private String schema;
    private String tableName;
    private Connection connection;

    @Getter
    private RelDataType relDataType;
    private List<RelDataType> colunmTypes;
    private List<String> columnNames;

    public LogicTable(String schema, String tableName, Connection connection) {
        super(Object[].class);
        this.schema = schema;
        this.tableName = tableName;
        this.connection = connection;
        relToSqlConverter = new RelToSqlConverter(SqlDialect.DatabaseProduct.MYSQL.getDialect());
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {

        final TableScan relNode = new EnumerableTableScan(context.getCluster(),
                context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
                relOptTable, (Class) getElementType());

        if (null == mysqlReader) {
            final String sql = relToSqlConverter.visit(relNode).asStatement().toString();
            mysqlReader = new LogicRowsReaderImpl(sql, connection, this);
        }

        return relNode;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        //get meta data from mysql;
        if (null != relDataType) {
            return relDataType;
        }

        //try to get table meta data from db;
        try {
            PreparedStatement preparedStatement =
                    connection.prepareStatement(MYSQL_COLUMN_META_SQL);

            preparedStatement.setString(1, schema);
            preparedStatement.setString(2, tableName);
            ResultSet r = preparedStatement.executeQuery();

            columnNames = Lists.newArrayList();
            colunmTypes = Lists.newArrayList();
            while (r.next()) {
                //FIXME Ignore column case
                columnNames.add(r.getString(1));
                final String columnTypeString = r.getString(2);
                final Class c = JavaTypeToSqlTypeConversion.getJavaTypeBySqlType(columnTypeString);
                colunmTypes.add(typeFactory.createJavaType(c));
            }

            relDataType = typeFactory.createStructType(colunmTypes, columnNames);
            return relDataType;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
