rm -r /home/itv003722/E2E/MarketAnalysis/MA_Outputs
mkdir /home/itv003722/E2E/MarketAnalysis/MA_Outputs

###Setting up hive metastore (this is specific to ITVarsity labs
hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse;"

#Step-4
###Removing & Creating MA Folder in HDFS under warehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/MA
hdfs dfs -mkdir -p /user/itv003722/warehouse/MA

#Transfer and store a data file from local systems to the Hadoop file system
hdfs dfs -put /home/itv003722/E2E/MarketAnalysis/Dataset.csv /user/itv003722/warehouse/MA/bank_data.csv


#creation of HiveDB
hive -f  /home/itv003722/E2E/MarketAnalysis/HiveDB.hql > /home/itv003722/E2E/MarketAnalysis/HiveDB.txt

# #Spark EDA
# export SPARK_MAJOR_VERSION=3
# spark-submit /home/itv003722/E2E/MarketAnalysis/MA_Spark_Analysis.py > /home/itv003722/E2E/MarketAnalysis/MA_Outputs/MA_SparkSQL_EDA.txt