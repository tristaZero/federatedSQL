## FederatedSQL
A repository for federatedSQL and SQL optimization using Apache Calcite.

## Functions
- Support SQL queries that join tables from different datasets 
- SQL query optimization

## How to run?
There two executors to choose.

### CalciteJDBCExecutor

This executor used Calcite JDBC driver to execute SQL.
1. Use `CalciteJDBCExecutor` to execute federated SQL queries
2. `CalciteJDBCExecutorTest is an example
    
### CalciteRawExecutor

This executor skips Calcite JDBC driver and call the crude parse, optimize, execute API by itself.
1. Use `CalciteRawExecutor` to execute federated SQL queries
2. `CalciteRawExecutorTest is an example
