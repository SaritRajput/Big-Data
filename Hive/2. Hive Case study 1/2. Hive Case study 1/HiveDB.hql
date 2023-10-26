SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
SET mapreduce.input.fileinputformat.split.maxsize = 67108864;
SET mapreduce.map.memory.mb = 2048;
SET mapreduce.reduce.memory.mb = 2048;
SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;


DROP DATABASE IF EXISTS DataCo CASCADE;
CREATE DATABASE DataCo;
USE DataCo;

CREATE EXTERNAL TABLE categories STORED AS AVRO LOCATION '/user/itv003722/warehouse/DataCo/categories' TBLPROPERTIES ('avro.schema.url'='/user/itv003722/avsc/categories.avsc');

CREATE EXTERNAL TABLE customers STORED AS AVRO LOCATION '/user/itv003722/warehouse/DataCo/customers' TBLPROPERTIES ('avro.schema.url'='/user/itv003722/avsc/customers.avsc');


CREATE EXTERNAL TABLE departments STORED AS AVRO LOCATION '/user/itv003722/warehouse/DataCo/departments' TBLPROPERTIES ('avro.schema.url'='/user/itv003722/avsc/departments.avsc');

CREATE EXTERNAL TABLE order_items STORED AS AVRO LOCATION '/user/itv003722/warehouse/DataCo/order_items' TBLPROPERTIES ('avro.schema.url'='/user/itv003722/avsc/order_items.avsc');

CREATE EXTERNAL TABLE orders STORED AS AVRO LOCATION '/user/itv003722/warehouse/DataCo/orders' TBLPROPERTIES ('avro.schema.url'='/user/itv003722/avsc/orders.avsc');

CREATE EXTERNAL TABLE products STORED AS AVRO LOCATION '/user/itv003722/warehouse/DataCo/products' TBLPROPERTIES ('avro.schema.url'='/user/itv003722/avsc/products.avsc');
