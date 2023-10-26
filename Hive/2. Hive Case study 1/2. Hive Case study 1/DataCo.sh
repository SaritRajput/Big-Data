###removing .avsc & .java files

find . -name "*.avsc" -exec rm {} \;
find . -name "*.java" -exec rm {} \;

#step-1

rm -r /home/itv003722/Hive/DataCo/DataCo_Outputs
mkdir /home/itv003722/Hive/DataCo/DataCo_Outputs


###Setting up hive metastore (this is specific to ITVarsity labs)

hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse;"

#Step-4
###Removing & Creating Capstone Folder in HDFS underwarehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/DataCo
hdfs dfs -mkdir -p /user/itv003722/warehouse/DataCo

sqoop import-all-tables  -Dmapreduce.job.user.classpath.first=true --connect "jdbc:mysql://g02.itversity.com/retail_db"  --username retail_user --password itversity  --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/itv003722/warehouse/DataCo/ -m 1 

ls -l *.avsc > /home/itv003722/Hive/DataCo/DataCo_Outputs/INlocalavroFiles
hdfs dfs -ls /user/itv003722/warehouse/DataCo > /home/itv003722/Hive/DataCo/DataCo_Outputs/HDFSFILE
hadoop fs -ls /user/itv003722/warehouse/DataCo/categories > /home/itv003722/Hive/DataCo/DataCo_Outputs/HDFSCatagoiesFILE
#Step-6
###moving all avsc files

hdfs dfs -rm -r /user/itv003722/avsc
hdfs dfs -mkdir -p /user/itv003722/avsc

hdfs dfs -put  /home/itv003722/categories.avsc /user/itv003722/avsc/categories.avsc
hdfs dfs -put  /home/itv003722/customers.avsc /user/itv003722/avsc/customers.avsc
hdfs dfs -put  /home/itv003722/departments.avsc /user/itv003722/avsc/departments.avsc
hdfs dfs -put  /home/itv003722/order_items.avsc /user/itv003722/avsc/order_items.avsc
hdfs dfs -put  /home/itv003722/orders.avsc /user/itv003722/avsc/orders.avsc
hdfs dfs -put  /home/itv003722/products.avsc /user/itv003722/avsc/products.avsc

###changing access writes

hadoop fs -chmod +rwx /user/itv003722/avsc/*
hadoop fs -chmod +rwx /user/itv003722/warehouse/DataCo/*


#creation of HiveDB
hive -f  /home/itv003722/Hive/DataCo/HiveDB.hql > /home/itv003722/Hive/DataCo/DataCo_Outputs/DataCo_HiveDB.txt

#Step-9

#Checking tables info
hive -f /home/itv003722/Hive/DataCo/HiveSql.sql > /home/itv003722/Hive/DataCo/DataCo_Outputs/DataCo_HiveTable.txt




