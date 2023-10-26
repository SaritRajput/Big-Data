#T create Directory to store output
rm -r /home/itv003722/sparkdata/sensor/sensor_out
mkdir /home/itv003722/sparkdata/sensor/sensor_out

#Step-2
ls -l /home/itv003722/sparkdata/sensor > /home/itv003722/sparkdata/sensor/sensor_out




###Setting up hive metastore (this is specific to ITVarsity labs)

hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse;"

#Step-4
###Removing & Creating sensor Folder in HDFS under warehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/sensor
hdfs dfs -mkdir -p /user/itv003722/warehouse/sensor

###transfer the .csv file from local to hdfs
hdfs dfs -put /home/itv003722/sparkdata/sensor/hvc.csv /user/itv003722/warehouse/sensor/hvc.csv
hdfs dfs -put /home/itv003722/sparkdata/sensor/building.csv /user/itv003722/warehouse/sensor/building.csv

#creation of HiveDB
hive -f /home/itv003722/sparkdata/sensor/hiveDB.hql > /home/itv003722/sparkdata/sensor/sensor_out/Stock_HiveDB.txt


#Spark EDA
export SPARK_MAJOR_VERSION=3
spark-submit /home/itv003722/sparkdata/sensor/spark_sensor_analysis.py > /home/itv003722/sparkdata/sensors/pyspark__EDA.txt
