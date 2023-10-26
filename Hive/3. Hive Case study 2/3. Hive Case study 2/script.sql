SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
DROP DATABASE IF EXISTS stock_analysis CASCADE;

--creating the Database
create database stock_analysis;
use stock_analysis;

--Creating the Table
create table nyse(stock_exchange STRING,stock_symbol STRING,stock_date STRING,stock_price_open double,stock_price_high double,stock_price_low double,stock_price_close double,stock_volume double,stock_price_adj_close double)
row format delimited
fields terminated by ','
stored as textfile;

--load the table from local to HDFS
load data local inpath '/home/itv003722/stockanalysis/NYSE_daily_prices_Q.csv' into table nyse;

--calculating the covariance
select a.stock_symbol,b.stock_symbol,month(a.stock_date),
(avg(a.stock_price_high*b.stock_price_high)- (avg(a.stock_price_high)*avg(b.stock_price_high)))
from nyse a
join nyse b
where a.stock_date=b.stock_date and a.stock_symbol<b.stock_symbol and year(a.stock_date)=2008
group by a.stock_symbol,b.stock_symbol,month(a.stock_date);

