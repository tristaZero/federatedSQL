
## FederatedSQL
A repository for federated SQL and SQL optimization using Apache Calcite.

## Functions
- Support SQL queries for `sharding table` in `sharding schema`
- Support SQL queries that join tables from different datasets 
- SQL query optimization

## How to run?
There two executors to choose.

### CalciteJDBCExecutor

This executor used Calcite JDBC driver to execute SQL.
1. Use `CalciteJDBCExecutor` to execute federated SQL queries
2. `CalciteJDBCExecutorTest` is an example
    
### CalciteRawExecutor

This executor skips Calcite JDBC driver and call the crude parse, optimize, execute API by itself.
1. Use `CalciteRawExecutor` to execute federated SQL queries
2. `CalciteRawExecutorTest` is an example


### Instruction for unit tests

#### Introduction
These unit tests to test simple SQL and Join SQL for `Logic table` in `Logic schema`.

- Logic schema (for users) consists of many actual databases in your MySQL instance

- Logic table (for users) is made of actual tables existing in your actual databases

#### Preparation
1. Start MySQL service
2. Execute SQLs below
```sql
CREATE DATABASE demo_ds_0;
CREATE DATABASE demo_ds_1;

USE demo_ds_0;

CREATE TABLE `t_order_0` (
  `order_id` int NOT NULL,
  `user_id` int NOT NULL,
  `status` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB;
 CREATE TABLE `t_order_item_0` (
  `order_item_id` int NOT NULL,
  `order_id` int NOT NULL,
  `user_id` int NOT NULL,
  `status` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`order_item_id`)
) ENGINE=InnoDB;

INSERT INTO t_order_0 SET order_id = 2, user_id = 2;
INSERT INTO t_order_item_0 SET order_id = 2, user_id = 2, order_item_id = 2;

use demo_ds_1;

CREATE TABLE `t_order_1` (
  `order_id` int NOT NULL,
  `user_id` int NOT NULL,
  `status` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB;
 CREATE TABLE `t_order_item_1` (
  `order_item_id` int NOT NULL,
  `order_id` int NOT NULL,
  `user_id` int NOT NULL,
  `status` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`order_item_id`)
) ENGINE=InnoDB;

INSERT INTO t_order_1 SET order_id = 1, user_id = 1;
INSERT INTO t_order_item_2 SET order_id = 1, user_id = 1, order_item_id = 1;
```
3. Update configuration
Update JdbcUrl, password, username with the properties of your database instance in `setUp()` of `CalciteRawExecutorTest` or `CalciteJDBCExecutorTest`

4. Run unit tests
Run `CalciteRawExecutorTest` or `CalciteJDBCExecutorTest`
