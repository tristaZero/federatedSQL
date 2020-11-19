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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;

/**
 * Logic row enumerator.
 *
 * @author panjuan
 * @author zhaojun
 */
public final class LogicRowEnumerator implements Enumerator<Object[]> {
    
    private Object[] currentRow;
    
    public LogicRowEnumerator(final MergedTupleResultSet resultSet) {
        this.resultSet = resultSet;
    }
    
    @Override
    public Object[] current() {
        return currentRow;
    }
    
    @Override
    public boolean moveNext() {
        if (resultSet.next()) {
            setCurrentRow();
            return true;
        }
        currentRow = null;
        return false;
    }
    
    private void setCurrentRow() {
        STMTuple tuple = resultSet.getSTMTuple();
        Object[] result = new Object[tuple.getRowVersion().getData().size()];
        try {
            TableMetaData tableMetaData = STMResourceManager.getInstance().getTableMetaData(tuple.getRowKey().getDataSourceName(), tuple.getRowKey().getTableName());
            currentRow = DataConvertUtils.convertDataMap(tuple.getRowVersion().getData(), tableMetaData).values().toArray(result);
        } catch (final STMTargetResourceException ex) {
            throw new STMException(ex);
        }
    }
    @Override
    public void reset() {
    }
    
    @Override
    public void close() {
        try {
            resultSet.close();
            currentRow = null;
        } catch (final Exception ex) {
            throw new STMMergeResultSetException(ex);
        }
    }
}
