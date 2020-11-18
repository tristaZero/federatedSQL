/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package federated.sql.schema;

import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.metadata.schema.builder.loader.TableMetaDataLoader;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract logic table.
 */
@Getter
public abstract class AbstractLogicTable extends AbstractTable {
    
    private final Collection<DataNode> dataNodes = new LinkedList<>();
    
    private final RelProtoDataType relProtoDataType;
    
    public AbstractLogicTable(final Map<String, DataSource> dataSources, final Collection<DataNode> dataNodes, final DatabaseType databaseType) throws SQLException {
        this.dataNodes.addAll(dataNodes);
        DataNode dataNode = dataNodes.iterator().next();
        Optional<TableMetaData> tableMetaData = TableMetaDataLoader.load(dataSources.get(dataNode.getDataSourceName()), dataNode.getTableName(), databaseType);
        if (!tableMetaData.isPresent()) {
            throw new RuntimeException("No table metaData.");
        }
        relProtoDataType = getRelDataType(tableMetaData.get());
    }
    
    private RelProtoDataType getRelDataType(final TableMetaData tableMetaData) {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for (Map.Entry<String, ColumnMetaData> entry : tableMetaData.getColumns().entrySet()) {
            SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(entry.getValue().getDataType());
            fieldInfo.add(entry.getKey(), null == sqlTypeName ? typeFactory.createUnknownType() : typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true));
        }
        return RelDataTypeImpl.proto(fieldInfo.build());
    }
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return relProtoDataType.apply(typeFactory);
    }
}
