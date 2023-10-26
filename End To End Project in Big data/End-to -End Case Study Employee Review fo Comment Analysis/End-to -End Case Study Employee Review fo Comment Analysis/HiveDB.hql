SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
DROP DATABASE IF EXISTS comment_analysis CASCADE;
CREATE DATABASE comment_analysis;
USE comment_analysis;


create table emp_reviews
(  
    sno STRING,
    company STRING,
    location STRING,
    dates STRING,
    job_title STRING,
    summary STRING,
    pros STRING,
    cons STRING,
    overall_ratings STRING,
    work_balance_stars STRING,
    culture_values_stars STRING,
    carrer_opportunities_stars STRING,
    comp_benefit_stars STRING,
    senior_mangemnet_stars STRING 
  
)
row format delimited
fields terminated by ','
stored as textfile;


load data inpath '/user/itv003722/warehouse/CA/emp_data.csv' OVERWRITE  into table emp_reviews; 



create table emp_reviews2
as
select cast(sno as int) as index,company,substr(location,(locate('(',location,1)+1),(locate(')',location,1)-1)-locate('(',location,1)) as country,substr(location,1,locate('(',location,1)-1) as state ,substr(dates,instr(dates,';')+1)  as year,job_title,summary
,pros,cons,cast(overall_ratings as int),cast(work_balance_stars as int),cast(culture_values_stars as int),cast(carrer_opportunities_stars as int),cast(comp_benefit_stars as int),cast(senior_mangemnet_stars as int)
from emp_reviews; 





describe emp_reviews2;

select percentile(cast(overall_ratings as bigint), 0.5) from emp_reviews;
select percentile(cast(work_balance_stars as BIGINT), 0.5) from emp_reviews;
select percentile(cast(culture_values_stars as BIGINT), 0.5) from emp_reviews;
select percentile(cast(carrer_opportunities_stars as BIGINT), 0.5) from emp_reviews;
select percentile(cast(comp_benefit_stars as BIGINT), 0.5) from emp_reviews;
select percentile(cast(senior_mangemnet_stars as BIGINT), 0.5) from emp_reviews;

create table review_notnull
as select index,company,country,state,year,job_title,summary,pros,cons,nvl(overall_ratings,4) overall_rating,nvl(work_balance_stars,3) as work_balance,
nvl(culture_values_stars,4) as cul_val,nvl(carrer_opportunities_stars,4) as  carrier_op,nvl(comp_benefit_stars,4) as comp_benefit,
nvl(senior_mangemnet_stars,3) as  sm
from 
emp_reviews2;



set hive.mapred.mode = strict 
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing=true;

create table review(index INT,company STRING,state STRING, year STRING, 
job_title STRING,summary STRING,pros STRING,cons STRING,overall_ratings INT,work_balance_stars INT,culture_values_stars INT,carrer_opportunities_stars INT,comp_benefit_stars INT,senior_mangemnet_stars INT) partitioned by (country STRING)
clustered by (year) INTO 10 buckets row format delimited fields terminated by ',' stored as textfile;


set hive.mapred.mode=unstrict;

from review_notnull txn INSERT OVERWRITE TABLE review PARTITION(country)
select txn.index,txn.company,txn.state,txn.year, txn.job_title,txn.summary,txn.pros,txn.cons,
txn.overall_rating, txn.work_balance,txn.cul_val,txn.carrier_op, 
txn.comp_benefit,txn.sm,txn.country DISTRIBUTE BY country;








