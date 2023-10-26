#Step -1
rm -r /home/itv003722/stockanalysis/stock_out
mkdir /home/itv003722/stockanalysis/stock_out



###Setting up hive metastore (this is specific to ITVarsity labs)

hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse;"

#Step-4
###Removing & Creating Capstone Folder in HDFS underwarehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/stockanalysis
hdfs dfs -mkdir -p /user/itv003722/warehouse/stockanalysis


#creation of HiveDB
hive -f /home/itv003722/stockanalysis/script.sql > /home/itv003722/stockanalysis/stock_out/Stock_Hive.txt






