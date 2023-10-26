

rm -r /home/itv003722/E2E/CommentAnalysis/CA_Outputs
mkdir /home/itv003722/E2E/CommentAnalysis/CA_Outputs

###Setting up hive metastore (this is specific to ITVarsity labs
hive -e "set hive.metastore.warehouse.dir = /user/itv003722/warehouse;"


###Removing & Creating CA Folder in HDFS underwarehouse directory

hdfs dfs -rm -r /user/itv003722/warehouse/CA
hdfs dfs -mkdir /user/itv003722/warehouse/CA

#Transfer and store a data file from local systems to the Hadoop file system
hdfs dfs -put /home/itv003722/E2E/CommentAnalysis/employee_data.csv /user/itv003722/warehouse/CA/emp_data.csv

#creation of HiveDB
hive -f  /home/itv003722/E2E/CommentAnalysis/HiveDB.hql > /home/itv003722/E2E/CommentAnalysis/HiveDB.txt

#Performing Exploratory Data Analysis
hive -f  /home/itv003722/E2E/CommentAnalysis/EDAHQL.hql > /home/itv003722/E2E/CommentAnalysis/CA_Outputs/EDAHQL.txt

# #Spark EDA
# export SPARK_MAJOR_VERSION=3
# spark-submit //home/itv003722/E2E/CommentAnalysis/Employee_Spark_analysis.py > /home/itv003722/E2E/CommentAnalysis/CA_Outputs/Employee_SparkSQL_EDA.txt

