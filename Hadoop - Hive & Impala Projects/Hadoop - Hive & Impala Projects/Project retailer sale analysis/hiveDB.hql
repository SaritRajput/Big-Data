SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
DROP DATABASE IF EXISTS online_retail;
CREATE DATABASE online_retail;
USE online_retail;

drop table online_retail;

create table online_retail(invoice_no bigint,stock_code STRING,description STRING,quantity int,invoice_date STRING ,unit_price DOUBLE,customer_id bigint,country STRING)
row format delimited
fields terminated by '\t'
stored as textfile
TBLPROPERTIES ("skip.header.line.count"="1");

load data inpath '/user/itv003722/warehouse/onlineRetailer/online_retail3.txt' OVERWRITE  into table online_retail; 


