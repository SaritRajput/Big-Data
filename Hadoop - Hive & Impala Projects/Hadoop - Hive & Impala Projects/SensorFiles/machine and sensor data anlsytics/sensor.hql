`SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
DROP DATABASE IF EXISTS sensordb CASCADE;
CREATE DATABASE sensordb;
USE sensordb;

create table hvc
(     
  s_date STRING,
  s_time STRING,
  target_temp int,
  actual_temp int,
  system int,
  system_age int,
  building_id int
  
)
row format delimited
fields terminated by ','
stored as textfile;

load data inpath '/user/itv003722/warehouse/hvc.csv' OVERWRITE  into table hvc; 





create table building
(     
  buildingID int,
  buildingMgr STRING,
  buildingAge int,
  hvacproduct STRING,
  country STRING
)
row format delimited
fields terminated by ','
stored as textfile;


load data inpath '/user/itv003722/warehouse/building.csv' OVERWRITE  into table building; 
