package federated.sql.schema;


import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.shardingsphere.infra.config.datasource.DataSourceParameter;
import org.apache.shardingsphere.infra.datanode.DataNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import static federated.sql.metadata.LogicSchemaConstants.COMMA_SEPARATOR;
import static federated.sql.metadata.LogicSchemaConstants.DATA_SOURCES;
import static federated.sql.metadata.LogicSchemaConstants.DOT_SEPARATOR;
import static federated.sql.metadata.LogicSchemaConstants.URL;
import static federated.sql.metadata.LogicSchemaConstants.USER_NAME;


/**
 * Logic schema factory.
 *
 */
public class LogicSchemaFactory implements SchemaFactory {

    public Schema create(final SchemaPlus parentSchema, final String name, final Map<String, Object> operand) {
        return new LogicSchema(getDataSourceParameters(operand), getDataNodes(operand));
    }
    
    private Map<String, DataSourceParameter> getDataSourceParameters(final Map<String, Object> operand) {
        Map<String, DataSourceParameter> result = new LinkedHashMap<>();
        for (Entry<String, Object> entry : operand.entrySet()) {
            if (entry.getKey().startsWith(DATA_SOURCES))  {
                fillDataSourceParameters(result, entry);
            }
        }
        return result;
    }
    
    private void fillDataSourceParameters(final Map<String, DataSourceParameter> dataSourceParameters, final Entry<String, Object> operand) {
        String[] parameters = operand.getKey().split(DOT_SEPARATOR);
        String dataSourceName = parameters[1];
        if (!dataSourceParameters.containsKey(dataSourceName)) {
            dataSourceParameters.put(dataSourceName, new DataSourceParameter());
        }
        if (URL.equals(parameters[2])) {
            dataSourceParameters.get(dataSourceName).setUrl(String.valueOf(operand.getValue()));
        } else if (USER_NAME.equals(parameters[2])) {
            dataSourceParameters.get(dataSourceName).setUsername(String.valueOf(operand.getValue()));
        } else {
            dataSourceParameters.get(dataSourceName).setPassword(String.valueOf(operand.getValue()));
        }
    }
    
    private Map<String, Collection<DataNode>> getDataNodes(final Map<String, Object> operand) {
        Map<String, Collection<DataNode>> result = new LinkedHashMap<>();
        for (Entry<String, Object> entry : operand.entrySet()) {
            if (entry.getKey().startsWith(DATA_SOURCES))  {
                fillDataNodes(result, entry);
            }
        }
        return result;
    }
    
    private void fillDataNodes(final Map<String, Collection<DataNode>> dataNodes, final Entry<String, Object> operand) {
        String[] parameters = operand.getKey().split(DOT_SEPARATOR);
        String tableName = parameters[1];
        if (!dataNodes.containsKey(tableName)) {
            String[] dataNodesStr = operand.getValue().toString().split(COMMA_SEPARATOR);
            dataNodes.put(tableName, getDataNodes(Arrays.asList(dataNodesStr)));
        }
    }
    
    private Collection<DataNode> getDataNodes(final Collection<String> dataNodes) {
        Collection<DataNode> result = new LinkedList<>();
        for (String each : dataNodes) {
            result.add(new DataNode(each));
        }
        return result;
    }
}
