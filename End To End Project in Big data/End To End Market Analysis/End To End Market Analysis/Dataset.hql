SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
DROP DATABASE IF EXISTS bank_domain CASCADE;
CREATE DATABASE bank_domain;
USE bank_domain;


create table market_analysis
(    age INT,
    job STRING,
    marital STRING,
    education STRING,
    default STRING,
    balance DOUBLE,
    housing STRING,
    loan STRING,
    contact STRING,
    day STRING,
    month STRING,
    duration bigint,
    campaign INT,
    pdays INT,
    previous INT,
    poutcome STRING,
    y String
)
row format delimited
fields terminated by ','
stored as textfile;

load data inpath '/user/itv003722/warehouse/MA/bank_data.csv' OVERWRITE  into table market_analysis; 

