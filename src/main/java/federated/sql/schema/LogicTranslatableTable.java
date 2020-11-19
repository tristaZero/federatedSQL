package federated.sql.schema;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * Logic translatable table.
 *
 */
public final class LogicTranslatableTable extends AbstractTable implements TranslatableTable {

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new EnumerableTableScan(context.getCluster(),
                context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
                relOptTable, Object[].class);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }
}
