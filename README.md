
## FederatedSQL
A repository for federated SQL and SQL optimization leveraging Apache Calcite.

## Functions
- Support SQL queries for `sharding table` in `sharding schema`
- Support SQL queries that join tables from different database instances 
- SQL query optimization

## How to run?
There two executors to test.

### CalciteJDBCExecutor

This executor executes SQL with the help of the Calcite JDBC driver.
1. Use `CalciteJDBCExecutor` to execute federated SQL queries
2. `CalciteJDBCExecutorTest` is an example
    
### CalciteRawExecutor

This executor skips the Calcite JDBC driver and calls the crude parse(), optimize() and execute() by itself. As you know, I write it diving into the source code of Apache Calcite. Why did I want to do that? The reason is that I am considering using [ShardingSphere Parser](https://shardingsphere.apache.org/document/current/en/features/sharding/use-norms/parser/) to have broad SQL support.
1. Use `CalciteRawExecutor` to execute federated SQL queries
2. `CalciteRawExecutorTest` is an example

### Unit tests

#### Introduction
These unit tests to test simple SQL and Join SQL for `Logic table` in `Logic schema`.

- Logic schema (for users) consists of many actual databases in your MySQL instance
- Logic table (for users) is made of actual tables existing in your actual databases

In this example, we will have two `Logic table`, namely, `t_order` and `t_order_item` in `Logic schema`, i.e., `sharding`. Here is the distribution fo them,
```
   ds0                    ds1
t_order_0            t_order_1
t_order_item_0   t_order_item_1
```
It is apparent that t_oder (logic table) is made of `ds0.t_order_0` and `ds1.t_order_1` (actual tables). Similarly, `ds0.t_order_item_0` and `ds1.t_order_item_1` makes up t_order_item. Hense, users just know there are two logic tables, i.e., t_order and t_order_item in `sharding` database without attention to its actual tables..

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
