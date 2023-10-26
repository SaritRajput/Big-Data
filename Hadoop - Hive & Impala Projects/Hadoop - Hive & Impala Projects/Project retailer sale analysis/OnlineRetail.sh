#Step -1
rm -r /home/itv003722/Hive/stock_out
mkdir /home/itv003722/Hive/stock_out

#Step-2
ls -l /home/itv003722/Hive/OnlineRetail > /home/itv003722/Hive/OnlineRetail/stock_out


###Setting up hive metastore (this is specific to ITVarsity labs)

hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse/;"

#Step-4
###Removing & Creating Capstone Folder in HDFS underwarehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/onlineRetailer
hdfs dfs -mkdir -p /user/itv003722/warehouse/onlineRetailer

###Transfering all the tables from PostgreSQL database to HDFS.

#creation of HiveDB

hdfs dfs -put /home/itv003722/Hive/OnlineRetail3.txt /user/itv003722/warehouse/onlineRetailer/online_retail3.txt

#creation of HiveDB
hive -f /home/itv003722/Hive/hiveDB.hql > /home/itv003722/Hive/stock_out/Stock_HiveDB.txt



Spark EDA
export SPARK_MAJOR_VERSION=3
spark-submit /home/itv003722/Hive//Retailer_Spark_Analytics.py > /home/itv003722/Hive/stock_out/Retailer_Spark_Analytics.txt














