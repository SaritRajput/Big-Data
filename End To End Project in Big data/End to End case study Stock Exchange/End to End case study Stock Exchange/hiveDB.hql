SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
DROP DATABASE IF EXISTS BDSH_PROJECT CASCADE;
CREATE DATABASE BDSH_PROJECT;
USE BDSH_PROJECT;


create table Stockcompanies(symbol STRING,security STRING,sector STRING,sub_industry STRING,headquarter STRING)
row format delimited
fields terminated by ','
stored as textfile;

create table StockPrices(training_date DATE,symbol STRING,open DOUBLE,close DOUBLE,low DOUBLE,high DOUBLE,volume int)
row format delimited
fields terminated by ','
stored as textfile;


CREATE TABLE stock_detail(year int ,month int ,symbol STRING,CompanyName STRING,state STRING,sector STRING,sub_industry STRING,
open DOUBLE,close DOUBLE,low DOUBLE,high DOUBLE,volume int);
insert overwrite table stock_detail
select year,month,symbol,CompanyName,state,sector,sub_industry, Open, close, low, high, volume
from 
(
select year(sp.training_date) as year,month(sp.training_date) as month ,s.symbol,s.security as CompanyName,substr(s.headquarter,instr(s.headquarter,';')+1) as state,s.sector,s.sub_industry,round(avg(sp.open),2) as open,round(avg(sp.close),2) as close,
round(avg(sp.low),2) as low,round(avg(sp.high),2) as high,round(avg(volume),2) as volume from stockcompanies s
join stockprices sp on s.symbol=sp.symbol
group by sp.training_date,s.symbol,s.security,s.headquarter,s.sector,s.sub_industry )
a
group by  a.year,a.month,a.symbol,a.CompanyName,a.state,a.sector,a.sub_industry,open, close, low, high, volume

;







