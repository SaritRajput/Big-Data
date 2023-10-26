#Step -1
rm -r /home/itv003722/E2E/stock_out
mkdir /home/itv003722/E2E/stock_output

#Step-2
ls -l /home/itv003722/E2E CS > /home/itv003722/E2E/stock_output

#Step-3
psql -h pg.itversity.com -d itv003722_retail_db -U itv003722_retail_user -p 5433 -a -q -f /home/itv003722/E2E/CreatePSQLTables.sql > /home/itv003722/E2E/stock_output/StockExchange_SQLTables.txt


###Setting up hive metastore (this is specific to ITVarsity labs)

hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse;"

#Step-4
###Removing & Creating Capstone Folder in HDFS underwarehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/StockExchange
hdfs dfs -mkdir -p /user/itv003722/warehouse/StockExchange

###Transfering all the tables from PostgreSQL database to HDFS.

#creation of HiveDB
hive -f /home/itv003722/E2E/hiveDB.hql > /home/itv003722/E2E/stock_output/Stock_HiveDB.txt


###Transfering all the tables from PostgreSQL database to HDFS.
sqoop import \
--connect "jdbc:postgresql://pg.itversity.com:5433/itv003722_retail_db"  \
--driver org.postgresql.Driver \
--table stockprices \
--username itv003722_retail_user  \
-P \
--target-dir /user/itv003722/warehouse/StockExchange/stockprices  -m 1 


sqoop import \
--connect "jdbc:postgresql://pg.itversity.com:5433/itv003722_retail_db"  \
--driver org.postgresql.Driver \
--table stockcompanies \
--username itv003722_retail_user  \
-P \
--target-dir /user/itv003722/warehouse/StockExchange/stockcompanies -m 1 

#creation of HiveDB
hive -f /home/itv003722/E2E/hiveDB.hql > /home/itv003722/E2E/stock_output/Stock_HiveDB.txt


hdfs dfs -cp ./warehouse/StockExchange/stockprices/part-m-00000 ./warehouse/bdsh_project.db/stockprices
hdfs dfs -cp ./warehouse/StockExchange/stockcompanies/part-m-00000 ./warehouse/bdsh_project.db/stockcompanies

#hdfs dfs -rm -r ./ warehouse/bdsh_project.db/stockcompanies/stockcompanies
#hdfs dfs -rm -r ./ warehouse/bdsh_project.db/stockprices/stockprices


#Spark ML
#export SPARK_MAJOR_VERSION=3.0.1
#spark-submit /home/itv003722/E2E/Spark_analysis.py > /home/itv003722/E2E/stock_output//Spark_analysis.txt









