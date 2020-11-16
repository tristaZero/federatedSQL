package federated.sql.schema;


import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

import static federated.sql.LogicSchemaConstants.PASSWORD;
import static federated.sql.LogicSchemaConstants.SCHEMA;
import static federated.sql.LogicSchemaConstants.URL;
import static federated.sql.LogicSchemaConstants.USER_NAME;


/**
 * Logic schema factory.
 *
 */
public class LogicSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String url = (String) operand.get(URL);
        String username = (String) operand.get(USER_NAME);
        String password = (String) operand.get(PASSWORD);
        String schema = (String) operand.get(SCHEMA);
        return new LogicSchema(url, username, password, schema);
    }
}
