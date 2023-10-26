{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "8e5da68a",
   "metadata": {},
   "outputs": [],
   "source": [
    "# # **Loading Hive Tables and Data Preparation for Analysis**\n",
    "\n",
    "import pyspark.sql.functions as F\n",
    "from pyspark.sql.functions import col, year, to_date, greatest, count, max\n",
    "from pyspark.sql import SparkSession"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "e9b51a7d",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import to_timestamp"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "ee999396",
   "metadata": {},
   "outputs": [],
   "source": [
    "spark = SparkSession.builder.appName(\"Sensor Data Analytics\").config(\n",
    "    \"spark.ui.port\", \"0\").config(\n",
    "        \"spark.sql.catalogImplementation=hive\").config(\n",
    "        \"spark.sql.warehouse.dir\",\n",
    "        \"hdfs://nameservice1/user/itv003722/warehouse\").config(\n",
    "            \"spark.serializer\",\n",
    "    \"org.apache.spark.serializer.KryoSerializer\").enableHiveSupport().getOrCreate()\n",
    "spark.sparkContext.setLogLevel('OFF')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "6d013c22",
   "metadata": {},
   "outputs": [],
   "source": [
    "hvc=spark.table('sensordb.hvc')\n",
    "hvc.createOrReplaceTempView('hvc')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "6700ecf4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- s_date: string (nullable = true)\n",
      " |-- s_time: string (nullable = true)\n",
      " |-- target_temp: integer (nullable = true)\n",
      " |-- actual_temp: integer (nullable = true)\n",
      " |-- system: integer (nullable = true)\n",
      " |-- system_age: integer (nullable = true)\n",
      " |-- building_id: integer (nullable = true)\n",
      "\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+\n",
      "|    s_date| s_time|target_temp|actual_temp|system|system_age|building_id|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+\n",
      "|20-06-2013|0:13:20|         65|         79|    14|        28|          5|\n",
      "|26-06-2013|0:43:51|         70|         66|    15|        28|         11|\n",
      "|20-06-2013|0:00:01|         70|         73|    19|         5|         10|\n",
      "|26-06-2013|0:33:07|         67|         69|     4|        24|          1|\n",
      "|26-06-2013|0:13:19|         67|         66|    16|        30|          2|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "8000"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "hvc.printSchema()\n",
    "hvc.show(5)\n",
    "hvc.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "67e5b3cc",
   "metadata": {},
   "outputs": [],
   "source": [
    "#hvc.drop(col('hvc_date'))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "686052f2",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+--------+-----------+-----------+------+----------+-----------+\n",
      "|    s_date|  s_time|target_temp|actual_temp|system|system_age|building_id|\n",
      "+----------+--------+-----------+-----------+------+----------+-----------+\n",
      "|20-06-2013| 0:13:20|         65|         79|    14|        28|          5|\n",
      "|26-06-2013| 0:43:51|         70|         66|    15|        28|         11|\n",
      "|20-06-2013| 0:00:01|         70|         73|    19|         5|         10|\n",
      "|26-06-2013| 0:33:07|         67|         69|     4|        24|          1|\n",
      "|26-06-2013| 0:13:19|         67|         66|    16|        30|          2|\n",
      "|01-06-2013| 0:00:01|         66|         58|    13|        20|          4|\n",
      "|02-06-2013| 1:00:01|         69|         68|     3|        20|         17|\n",
      "|03-06-2013| 2:00:01|         70|         73|    17|        20|         18|\n",
      "|04-06-2013| 3:00:01|         67|         63|     2|        23|         15|\n",
      "|05-06-2013| 4:00:01|         68|         74|    16|         9|          3|\n",
      "|06-06-2013| 5:00:01|         67|         56|    13|        28|          4|\n",
      "|07-06-2013| 6:00:01|         70|         58|    12|        24|          2|\n",
      "|08-06-2013| 7:00:01|         70|         73|    20|        26|         16|\n",
      "|09-06-2013| 8:00:01|         66|         69|    16|         9|          9|\n",
      "|10-06-2013| 9:00:01|         65|         57|     6|         5|         12|\n",
      "|11-06-2013|10:00:01|         67|         70|    10|        17|         15|\n",
      "|12-06-2013|11:00:01|         69|         62|     2|        11|          7|\n",
      "|09-06-2013| 8:00:01|         66|         67|    12|        17|          4|\n",
      "|10-06-2013| 9:00:01|         67|         67|     2|         8|         17|\n",
      "|11-06-2013|10:00:01|         69|         60|    11|        29|          4|\n",
      "+----------+--------+-----------+-----------+------+----------+-----------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "hvc.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "350196d9",
   "metadata": {},
   "outputs": [],
   "source": [
    "building=spark.table('sensordb.building')\n",
    "building.createOrReplaceTempView('building')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "44d26f7b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- buildingid: integer (nullable = true)\n",
      " |-- buildingmgr: string (nullable = true)\n",
      " |-- buildingage: integer (nullable = true)\n",
      " |-- hvacproduct: string (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      "\n",
      "+----------+-----------+-----------+-----------+---------+\n",
      "|buildingid|buildingmgr|buildingage|hvacproduct|  country|\n",
      "+----------+-----------+-----------+-----------+---------+\n",
      "|         1|         M1|         25|     AC1000|      USA|\n",
      "|         2|         M2|         27|     FN39TG|   France|\n",
      "|         3|         M3|         28|     JDNS77|   Brazil|\n",
      "|         4|         M4|         17|     GG1919|  Finland|\n",
      "|         5|         M5|          3|    ACMAX22|Hong Kong|\n",
      "+----------+-----------+-----------+-----------+---------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "20"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "building.printSchema()\n",
    "building.show(5)\n",
    "building.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "641af9f0",
   "metadata": {},
   "outputs": [],
   "source": [
    "#calculate the temp_diff in HAVC \n",
    "hvc=hvc.withColumn('temp_diff',col(\"actual_temp\")-col(\"target_temp\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "e0920722",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+\n",
      "|    s_date| s_time|target_temp|actual_temp|system|system_age|building_id|temp_diff|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+\n",
      "|20-06-2013|0:13:20|         65|         79|    14|        28|          5|       14|\n",
      "|26-06-2013|0:43:51|         70|         66|    15|        28|         11|       -4|\n",
      "|20-06-2013|0:00:01|         70|         73|    19|         5|         10|        3|\n",
      "|26-06-2013|0:33:07|         67|         69|     4|        24|          1|        2|\n",
      "|26-06-2013|0:13:19|         67|         66|    16|        30|          2|       -1|\n",
      "|01-06-2013|0:00:01|         66|         58|    13|        20|          4|       -8|\n",
      "|02-06-2013|1:00:01|         69|         68|     3|        20|         17|       -1|\n",
      "|03-06-2013|2:00:01|         70|         73|    17|        20|         18|        3|\n",
      "|04-06-2013|3:00:01|         67|         63|     2|        23|         15|       -4|\n",
      "|05-06-2013|4:00:01|         68|         74|    16|         9|          3|        6|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+\n",
      "only showing top 10 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "hvc.show(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "8b15ca3b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>s_date</th><th>s_time</th><th>target_temp</th><th>actual_temp</th><th>system</th><th>system_age</th><th>building_id</th><th>temp_diff</th></tr>\n",
       "<tr><td>01-06-2013</td><td>0:00:01</td><td>66</td><td>58</td><td>13</td><td>20</td><td>4</td><td>-8</td></tr>\n",
       "<tr><td>06-06-2013</td><td>5:00:01</td><td>67</td><td>56</td><td>13</td><td>28</td><td>4</td><td>-11</td></tr>\n",
       "<tr><td>07-06-2013</td><td>6:00:01</td><td>70</td><td>58</td><td>12</td><td>24</td><td>2</td><td>-12</td></tr>\n",
       "<tr><td>10-06-2013</td><td>9:00:01</td><td>65</td><td>57</td><td>6</td><td>5</td><td>12</td><td>-8</td></tr>\n",
       "<tr><td>12-06-2013</td><td>11:00:01</td><td>69</td><td>62</td><td>2</td><td>11</td><td>7</td><td>-7</td></tr>\n",
       "<tr><td>11-06-2013</td><td>10:00:01</td><td>69</td><td>60</td><td>11</td><td>29</td><td>4</td><td>-9</td></tr>\n",
       "<tr><td>12-06-2013</td><td>11:00:01</td><td>70</td><td>63</td><td>14</td><td>6</td><td>7</td><td>-7</td></tr>\n",
       "<tr><td>13-06-2013</td><td>12:00:01</td><td>70</td><td>60</td><td>19</td><td>16</td><td>6</td><td>-10</td></tr>\n",
       "<tr><td>21-06-2013</td><td>14:33:07</td><td>70</td><td>64</td><td>8</td><td>17</td><td>3</td><td>-6</td></tr>\n",
       "<tr><td>29-06-2013</td><td>22:33:07</td><td>70</td><td>64</td><td>6</td><td>12</td><td>6</td><td>-6</td></tr>\n",
       "<tr><td>30-06-2013</td><td>23:33:07</td><td>69</td><td>60</td><td>10</td><td>9</td><td>2</td><td>-9</td></tr>\n",
       "<tr><td>01-06-2013</td><td>0:00:01</td><td>69</td><td>60</td><td>5</td><td>8</td><td>7</td><td>-9</td></tr>\n",
       "<tr><td>09-06-2013</td><td>6:43:51</td><td>68</td><td>58</td><td>12</td><td>5</td><td>2</td><td>-10</td></tr>\n",
       "<tr><td>28-06-2013</td><td>3:00:01</td><td>69</td><td>57</td><td>8</td><td>28</td><td>8</td><td>-12</td></tr>\n",
       "<tr><td>24-06-2013</td><td>17:13:20</td><td>69</td><td>63</td><td>15</td><td>18</td><td>8</td><td>-6</td></tr>\n",
       "<tr><td>22-06-2013</td><td>1:13:20</td><td>70</td><td>61</td><td>13</td><td>9</td><td>18</td><td>-9</td></tr>\n",
       "<tr><td>23-06-2013</td><td>2:13:20</td><td>70</td><td>55</td><td>6</td><td>8</td><td>5</td><td>-15</td></tr>\n",
       "<tr><td>24-06-2013</td><td>3:13:20</td><td>67</td><td>60</td><td>9</td><td>26</td><td>12</td><td>-7</td></tr>\n",
       "<tr><td>26-06-2013</td><td>5:13:20</td><td>67</td><td>55</td><td>15</td><td>4</td><td>12</td><td>-12</td></tr>\n",
       "<tr><td>28-06-2013</td><td>7:13:20</td><td>69</td><td>57</td><td>2</td><td>1</td><td>18</td><td>-12</td></tr>\n",
       "</table>\n",
       "only showing top 20 rows\n"
      ],
      "text/plain": [
       "+----------+--------+-----------+-----------+------+----------+-----------+---------+\n",
       "|    s_date|  s_time|target_temp|actual_temp|system|system_age|building_id|temp_diff|\n",
       "+----------+--------+-----------+-----------+------+----------+-----------+---------+\n",
       "|01-06-2013| 0:00:01|         66|         58|    13|        20|          4|       -8|\n",
       "|06-06-2013| 5:00:01|         67|         56|    13|        28|          4|      -11|\n",
       "|07-06-2013| 6:00:01|         70|         58|    12|        24|          2|      -12|\n",
       "|10-06-2013| 9:00:01|         65|         57|     6|         5|         12|       -8|\n",
       "|12-06-2013|11:00:01|         69|         62|     2|        11|          7|       -7|\n",
       "|11-06-2013|10:00:01|         69|         60|    11|        29|          4|       -9|\n",
       "|12-06-2013|11:00:01|         70|         63|    14|         6|          7|       -7|\n",
       "|13-06-2013|12:00:01|         70|         60|    19|        16|          6|      -10|\n",
       "|21-06-2013|14:33:07|         70|         64|     8|        17|          3|       -6|\n",
       "|29-06-2013|22:33:07|         70|         64|     6|        12|          6|       -6|\n",
       "|30-06-2013|23:33:07|         69|         60|    10|         9|          2|       -9|\n",
       "|01-06-2013| 0:00:01|         69|         60|     5|         8|          7|       -9|\n",
       "|09-06-2013| 6:43:51|         68|         58|    12|         5|          2|      -10|\n",
       "|28-06-2013| 3:00:01|         69|         57|     8|        28|          8|      -12|\n",
       "|24-06-2013|17:13:20|         69|         63|    15|        18|          8|       -6|\n",
       "|22-06-2013| 1:13:20|         70|         61|    13|         9|         18|       -9|\n",
       "|23-06-2013| 2:13:20|         70|         55|     6|         8|          5|      -15|\n",
       "|24-06-2013| 3:13:20|         67|         60|     9|        26|         12|       -7|\n",
       "|26-06-2013| 5:13:20|         67|         55|    15|         4|         12|      -12|\n",
       "|28-06-2013| 7:13:20|         69|         57|     2|         1|         18|      -12|\n",
       "+----------+--------+-----------+-----------+------+----------+-----------+---------+\n",
       "only showing top 20 rows"
      ]
     },
     "execution_count": 12,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#filter the condtion temp_diff<-5\n",
    "hvc.filter(col(\"temp_diff\")<-5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "79df2ca0",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>s_date</th><th>s_time</th><th>target_temp</th><th>actual_temp</th><th>system</th><th>system_age</th><th>building_id</th><th>temp_diff</th></tr>\n",
       "<tr><td>20-06-2013</td><td>0:13:20</td><td>65</td><td>79</td><td>14</td><td>28</td><td>5</td><td>14</td></tr>\n",
       "<tr><td>05-06-2013</td><td>4:00:01</td><td>68</td><td>74</td><td>16</td><td>9</td><td>3</td><td>6</td></tr>\n",
       "<tr><td>14-06-2013</td><td>13:00:01</td><td>69</td><td>78</td><td>6</td><td>26</td><td>2</td><td>9</td></tr>\n",
       "<tr><td>15-06-2013</td><td>14:00:01</td><td>65</td><td>71</td><td>7</td><td>4</td><td>6</td><td>6</td></tr>\n",
       "<tr><td>22-06-2013</td><td>15:33:07</td><td>67</td><td>76</td><td>6</td><td>9</td><td>11</td><td>9</td></tr>\n",
       "<tr><td>24-06-2013</td><td>17:33:07</td><td>70</td><td>80</td><td>6</td><td>28</td><td>17</td><td>10</td></tr>\n",
       "<tr><td>27-06-2013</td><td>20:33:07</td><td>65</td><td>80</td><td>5</td><td>4</td><td>9</td><td>15</td></tr>\n",
       "<tr><td>28-06-2013</td><td>21:33:07</td><td>70</td><td>79</td><td>4</td><td>21</td><td>14</td><td>9</td></tr>\n",
       "<tr><td>02-06-2013</td><td>1:00:01</td><td>67</td><td>74</td><td>3</td><td>10</td><td>16</td><td>7</td></tr>\n",
       "<tr><td>03-06-2013</td><td>2:00:01</td><td>68</td><td>80</td><td>4</td><td>13</td><td>15</td><td>12</td></tr>\n",
       "<tr><td>25-06-2013</td><td>17:00:01</td><td>68</td><td>74</td><td>2</td><td>22</td><td>18</td><td>6</td></tr>\n",
       "<tr><td>22-06-2013</td><td>8:33:07</td><td>66</td><td>76</td><td>9</td><td>1</td><td>16</td><td>10</td></tr>\n",
       "<tr><td>18-06-2013</td><td>17:43:51</td><td>67</td><td>73</td><td>6</td><td>11</td><td>12</td><td>6</td></tr>\n",
       "<tr><td>22-06-2013</td><td>10:13:19</td><td>65</td><td>71</td><td>9</td><td>23</td><td>13</td><td>6</td></tr>\n",
       "<tr><td>21-06-2013</td><td>0:13:20</td><td>65</td><td>75</td><td>14</td><td>1</td><td>1</td><td>10</td></tr>\n",
       "<tr><td>27-06-2013</td><td>6:13:20</td><td>68</td><td>74</td><td>18</td><td>17</td><td>12</td><td>6</td></tr>\n",
       "<tr><td>30-06-2013</td><td>9:13:20</td><td>68</td><td>74</td><td>4</td><td>9</td><td>13</td><td>6</td></tr>\n",
       "<tr><td>03-06-2013</td><td>12:13:20</td><td>70</td><td>80</td><td>8</td><td>15</td><td>4</td><td>10</td></tr>\n",
       "<tr><td>05-06-2013</td><td>14:13:20</td><td>68</td><td>79</td><td>6</td><td>25</td><td>10</td><td>11</td></tr>\n",
       "<tr><td>07-06-2013</td><td>16:13:20</td><td>66</td><td>72</td><td>3</td><td>15</td><td>15</td><td>6</td></tr>\n",
       "</table>\n",
       "only showing top 20 rows\n"
      ],
      "text/plain": [
       "+----------+--------+-----------+-----------+------+----------+-----------+---------+\n",
       "|    s_date|  s_time|target_temp|actual_temp|system|system_age|building_id|temp_diff|\n",
       "+----------+--------+-----------+-----------+------+----------+-----------+---------+\n",
       "|20-06-2013| 0:13:20|         65|         79|    14|        28|          5|       14|\n",
       "|05-06-2013| 4:00:01|         68|         74|    16|         9|          3|        6|\n",
       "|14-06-2013|13:00:01|         69|         78|     6|        26|          2|        9|\n",
       "|15-06-2013|14:00:01|         65|         71|     7|         4|          6|        6|\n",
       "|22-06-2013|15:33:07|         67|         76|     6|         9|         11|        9|\n",
       "|24-06-2013|17:33:07|         70|         80|     6|        28|         17|       10|\n",
       "|27-06-2013|20:33:07|         65|         80|     5|         4|          9|       15|\n",
       "|28-06-2013|21:33:07|         70|         79|     4|        21|         14|        9|\n",
       "|02-06-2013| 1:00:01|         67|         74|     3|        10|         16|        7|\n",
       "|03-06-2013| 2:00:01|         68|         80|     4|        13|         15|       12|\n",
       "|25-06-2013|17:00:01|         68|         74|     2|        22|         18|        6|\n",
       "|22-06-2013| 8:33:07|         66|         76|     9|         1|         16|       10|\n",
       "|18-06-2013|17:43:51|         67|         73|     6|        11|         12|        6|\n",
       "|22-06-2013|10:13:19|         65|         71|     9|        23|         13|        6|\n",
       "|21-06-2013| 0:13:20|         65|         75|    14|         1|          1|       10|\n",
       "|27-06-2013| 6:13:20|         68|         74|    18|        17|         12|        6|\n",
       "|30-06-2013| 9:13:20|         68|         74|     4|         9|         13|        6|\n",
       "|03-06-2013|12:13:20|         70|         80|     8|        15|          4|       10|\n",
       "|05-06-2013|14:13:20|         68|         79|     6|        25|         10|       11|\n",
       "|07-06-2013|16:13:20|         66|         72|     3|        15|         15|        6|\n",
       "+----------+--------+-----------+-----------+------+----------+-----------+---------+\n",
       "only showing top 20 rows"
      ]
     },
     "execution_count": 13,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "##filter the condtion temp_diff>5\n",
    "hvc.filter(col(\"temp_diff\")>5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "1fa97503",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import when"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "673b0ec5",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Create column  Temprange indicates whether the temp is Normal, cold ,Hot\n",
    "hvc_cond=hvc.withColumn(\"temprange\",\n",
    "              when(hvc.temp_diff<5,'Normal')\n",
    "              .when(hvc.temp_diff>-5,'Cold')\n",
    "              .when(hvc.temp_diff>5,'Hot')\n",
    "              .otherwise('Unknow'))\n",
    "                "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "97517fcc",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+---------+\n",
      "|    s_date| s_time|target_temp|actual_temp|system|system_age|building_id|temp_diff|temprange|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+---------+\n",
      "|20-06-2013|0:13:20|         65|         79|    14|        28|          5|       14|     Cold|\n",
      "|26-06-2013|0:43:51|         70|         66|    15|        28|         11|       -4|   Normal|\n",
      "|20-06-2013|0:00:01|         70|         73|    19|         5|         10|        3|   Normal|\n",
      "|26-06-2013|0:33:07|         67|         69|     4|        24|          1|        2|   Normal|\n",
      "|26-06-2013|0:13:19|         67|         66|    16|        30|          2|       -1|   Normal|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+---------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "hvc_cond.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "f7547d83",
   "metadata": {},
   "outputs": [],
   "source": [
    "#create a column of Extreme temp with specific condition\n",
    "hvc2=hvc_cond.withColumn('extreme_temp',\n",
    "              when((col(\"temp_diff\")>5) | (col(\"temp_diff\")<-5) ,1)\n",
    "              .otherwise(0)  \n",
    "    )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "66c39a6c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+---------+------------+\n",
      "|    s_date| s_time|target_temp|actual_temp|system|system_age|building_id|temp_diff|temprange|extreme_temp|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+---------+------------+\n",
      "|20-06-2013|0:13:20|         65|         79|    14|        28|          5|       14|     Cold|           1|\n",
      "|26-06-2013|0:43:51|         70|         66|    15|        28|         11|       -4|   Normal|           0|\n",
      "|20-06-2013|0:00:01|         70|         73|    19|         5|         10|        3|   Normal|           0|\n",
      "|26-06-2013|0:33:07|         67|         69|     4|        24|          1|        2|   Normal|           0|\n",
      "|26-06-2013|0:13:19|         67|         66|    16|        30|          2|       -1|   Normal|           0|\n",
      "+----------+-------+-----------+-----------+------+----------+-----------+---------+---------+------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "hvc2.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "14d5a39d",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Rename the column Building_id to buildingId \n",
    "hvc2=hvc2.withColumnRenamed('building_id','buildingid')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "ff38f1d1",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------+---------+------------+----------+-----------+-----------+-----------+---------+\n",
      "|temp_diff|temprange|extreme_temp|buildingid|buildingmgr|buildingage|hvacproduct|  country|\n",
      "+---------+---------+------------+----------+-----------+-----------+-----------+---------+\n",
      "|       14|     Cold|           1|         5|         M5|          3|    ACMAX22|Hong Kong|\n",
      "|       -4|   Normal|           0|        11|        M11|         14|     AC1000|  Belgium|\n",
      "|        3|   Normal|           0|        10|        M10|         23|    ACMAX22|    China|\n",
      "|        2|   Normal|           0|         1|         M1|         25|     AC1000|      USA|\n",
      "|       -1|   Normal|           0|         2|         M2|         27|     FN39TG|   France|\n",
      "|       -8|   Normal|           1|         4|         M4|         17|     GG1919|  Finland|\n",
      "|       -1|   Normal|           0|        17|        M17|         11|     FN39TG|    Egypt|\n",
      "|        3|   Normal|           0|        18|        M18|         25|     JDNS77|Indonesia|\n",
      "|       -4|   Normal|           0|        15|        M15|         19|    ACMAX22|   Israel|\n",
      "|        6|     Cold|           1|         3|         M3|         28|     JDNS77|   Brazil|\n",
      "+---------+---------+------------+----------+-----------+-----------+-----------+---------+\n",
      "only showing top 10 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#join the two table with comman col buildngid\n",
    "sensor_df=hvc2.join(building,on='buildingId').select('temp_diff','temprange','extreme_temp','buildingid','buildingmgr','buildingage','hvacproduct','country')\n",
    "sensor_df.show(10)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "07617392",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------+---------+-----------------+\n",
      "|     country|temprange|extrem_temp_count|\n",
      "+------------+---------+-----------------+\n",
      "|         USA|   Normal|              283|\n",
      "|         USA|     Cold|              110|\n",
      "|      Turkey|     Cold|              144|\n",
      "|      Turkey|   Normal|              270|\n",
      "|South Africa|   Normal|              263|\n",
      "+------------+---------+-----------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#count country wise extreme temp\n",
    "sensor_df.groupBy('country','temprange').agg(count(col('extreme_temp')).alias('extrem_temp_count')).orderBy(col('country').desc()).show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "id": "eef52c52",
   "metadata": {},
   "outputs": [],
   "source": [
    "# check the distinct country\n",
    "distinct_country=sensor_df.select('country').distinct()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "22251a1e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------+\n",
      "|     country|\n",
      "+------------+\n",
      "|   Singapore|\n",
      "|      Turkey|\n",
      "|     Germany|\n",
      "|      France|\n",
      "|   Argentina|\n",
      "|     Belgium|\n",
      "|     Finland|\n",
      "|       China|\n",
      "|   Hong Kong|\n",
      "|      Israel|\n",
      "|         USA|\n",
      "|      Mexico|\n",
      "|   Indonesia|\n",
      "|Saudi Arabia|\n",
      "|      Canada|\n",
      "|      Brazil|\n",
      "|   Australia|\n",
      "|       Egypt|\n",
      "|South Africa|\n",
      "+------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "distinct_country.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "id": "30f6396d",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>buildingmgr</th></tr>\n",
       "<tr><td>M19</td></tr>\n",
       "<tr><td>M4</td></tr>\n",
       "<tr><td>M7</td></tr>\n",
       "<tr><td>M14</td></tr>\n",
       "<tr><td>M6</td></tr>\n",
       "<tr><td>M18</td></tr>\n",
       "<tr><td>M17</td></tr>\n",
       "<tr><td>M11</td></tr>\n",
       "<tr><td>M15</td></tr>\n",
       "<tr><td>M1</td></tr>\n",
       "<tr><td>M20</td></tr>\n",
       "<tr><td>M13</td></tr>\n",
       "<tr><td>M5</td></tr>\n",
       "<tr><td>M2</td></tr>\n",
       "<tr><td>M3</td></tr>\n",
       "<tr><td>M12</td></tr>\n",
       "<tr><td>M10</td></tr>\n",
       "<tr><td>M9</td></tr>\n",
       "<tr><td>M16</td></tr>\n",
       "<tr><td>M8</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+-----------+\n",
       "|buildingmgr|\n",
       "+-----------+\n",
       "|        M19|\n",
       "|         M4|\n",
       "|         M7|\n",
       "|        M14|\n",
       "|         M6|\n",
       "|        M18|\n",
       "|        M17|\n",
       "|        M11|\n",
       "|        M15|\n",
       "|         M1|\n",
       "|        M20|\n",
       "|        M13|\n",
       "|         M5|\n",
       "|         M2|\n",
       "|         M3|\n",
       "|        M12|\n",
       "|        M10|\n",
       "|         M9|\n",
       "|        M16|\n",
       "|         M8|\n",
       "+-----------+"
      ]
     },
     "execution_count": 24,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\n",
    "sensor_df.select('buildingmgr').distinct()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "57dc77b1",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>country</th><th>count((temprange = Hot))</th></tr>\n",
       "<tr><td>Singapore</td><td>415</td></tr>\n",
       "<tr><td>Turkey</td><td>414</td></tr>\n",
       "<tr><td>Germany</td><td>363</td></tr>\n",
       "<tr><td>France</td><td>400</td></tr>\n",
       "<tr><td>Argentina</td><td>408</td></tr>\n",
       "<tr><td>Belgium</td><td>376</td></tr>\n",
       "<tr><td>Finland</td><td>814</td></tr>\n",
       "<tr><td>China</td><td>425</td></tr>\n",
       "<tr><td>Hong Kong</td><td>406</td></tr>\n",
       "<tr><td>Israel</td><td>399</td></tr>\n",
       "<tr><td>USA</td><td>393</td></tr>\n",
       "<tr><td>Mexico</td><td>383</td></tr>\n",
       "<tr><td>Indonesia</td><td>421</td></tr>\n",
       "<tr><td>Saudi Arabia</td><td>396</td></tr>\n",
       "<tr><td>Canada</td><td>380</td></tr>\n",
       "<tr><td>Brazil</td><td>406</td></tr>\n",
       "<tr><td>Australia</td><td>380</td></tr>\n",
       "<tr><td>Egypt</td><td>406</td></tr>\n",
       "<tr><td>South Africa</td><td>415</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+------------+------------------------+\n",
       "|     country|count((temprange = Hot))|\n",
       "+------------+------------------------+\n",
       "|   Singapore|                     415|\n",
       "|      Turkey|                     414|\n",
       "|     Germany|                     363|\n",
       "|      France|                     400|\n",
       "|   Argentina|                     408|\n",
       "|     Belgium|                     376|\n",
       "|     Finland|                     814|\n",
       "|       China|                     425|\n",
       "|   Hong Kong|                     406|\n",
       "|      Israel|                     399|\n",
       "|         USA|                     393|\n",
       "|      Mexico|                     383|\n",
       "|   Indonesia|                     421|\n",
       "|Saudi Arabia|                     396|\n",
       "|      Canada|                     380|\n",
       "|      Brazil|                     406|\n",
       "|   Australia|                     380|\n",
       "|       Egypt|                     406|\n",
       "|South Africa|                     415|\n",
       "+------------+------------------------+"
      ]
     },
     "execution_count": 36,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#count \n",
    "sensor_df.groupBy(\"country\").agg(count(col(\"temprange\")==\"Hot\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "461537dc",
   "metadata": {},
   "outputs": [],
   "source": [
    "result = sensor_df.groupBy(\"country\").agg(sum(when(col(\"temprange\")==\"Hot\", 1).otherwise(0)).alias(\"aaaaaaa\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "90db1bab",
   "metadata": {},
   "outputs": [],
   "source": [
    "#sensor_df.createOrReplaceTempView('sensor_df')\n",
    "sensor_df.registerTempTable(\"sensor_df\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "1b815d2e",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>temp_diff</th><th>temprange</th><th>extreme_temp</th><th>buildingid</th><th>buildingmgr</th><th>buildingage</th><th>hvacproduct</th><th>country</th></tr>\n",
       "<tr><td>14</td><td>Cold</td><td>1</td><td>5</td><td>M5</td><td>3</td><td>ACMAX22</td><td>Hong Kong</td></tr>\n",
       "<tr><td>-4</td><td>Normal</td><td>0</td><td>11</td><td>M11</td><td>14</td><td>AC1000</td><td>Belgium</td></tr>\n",
       "<tr><td>3</td><td>Normal</td><td>0</td><td>10</td><td>M10</td><td>23</td><td>ACMAX22</td><td>China</td></tr>\n",
       "<tr><td>2</td><td>Normal</td><td>0</td><td>1</td><td>M1</td><td>25</td><td>AC1000</td><td>USA</td></tr>\n",
       "<tr><td>-1</td><td>Normal</td><td>0</td><td>2</td><td>M2</td><td>27</td><td>FN39TG</td><td>France</td></tr>\n",
       "<tr><td>-8</td><td>Normal</td><td>1</td><td>4</td><td>M4</td><td>17</td><td>GG1919</td><td>Finland</td></tr>\n",
       "<tr><td>-1</td><td>Normal</td><td>0</td><td>17</td><td>M17</td><td>11</td><td>FN39TG</td><td>Egypt</td></tr>\n",
       "<tr><td>3</td><td>Normal</td><td>0</td><td>18</td><td>M18</td><td>25</td><td>JDNS77</td><td>Indonesia</td></tr>\n",
       "<tr><td>-4</td><td>Normal</td><td>0</td><td>15</td><td>M15</td><td>19</td><td>ACMAX22</td><td>Israel</td></tr>\n",
       "<tr><td>6</td><td>Cold</td><td>1</td><td>3</td><td>M3</td><td>28</td><td>JDNS77</td><td>Brazil</td></tr>\n",
       "<tr><td>-11</td><td>Normal</td><td>1</td><td>4</td><td>M4</td><td>17</td><td>GG1919</td><td>Finland</td></tr>\n",
       "<tr><td>-12</td><td>Normal</td><td>1</td><td>2</td><td>M2</td><td>27</td><td>FN39TG</td><td>France</td></tr>\n",
       "<tr><td>3</td><td>Normal</td><td>0</td><td>16</td><td>M16</td><td>23</td><td>AC1000</td><td>Turkey</td></tr>\n",
       "<tr><td>3</td><td>Normal</td><td>0</td><td>9</td><td>M9</td><td>11</td><td>GG1919</td><td>Mexico</td></tr>\n",
       "<tr><td>-8</td><td>Normal</td><td>1</td><td>12</td><td>M12</td><td>26</td><td>FN39TG</td><td>Finland</td></tr>\n",
       "<tr><td>3</td><td>Normal</td><td>0</td><td>15</td><td>M15</td><td>19</td><td>ACMAX22</td><td>Israel</td></tr>\n",
       "<tr><td>-7</td><td>Normal</td><td>1</td><td>7</td><td>M7</td><td>13</td><td>FN39TG</td><td>South Africa</td></tr>\n",
       "<tr><td>1</td><td>Normal</td><td>0</td><td>4</td><td>M4</td><td>17</td><td>GG1919</td><td>Finland</td></tr>\n",
       "<tr><td>0</td><td>Normal</td><td>0</td><td>17</td><td>M17</td><td>11</td><td>FN39TG</td><td>Egypt</td></tr>\n",
       "<tr><td>-9</td><td>Normal</td><td>1</td><td>4</td><td>M4</td><td>17</td><td>GG1919</td><td>Finland</td></tr>\n",
       "</table>\n",
       "only showing top 20 rows\n"
      ],
      "text/plain": [
       "+---------+---------+------------+----------+-----------+-----------+-----------+------------+\n",
       "|temp_diff|temprange|extreme_temp|buildingid|buildingmgr|buildingage|hvacproduct|     country|\n",
       "+---------+---------+------------+----------+-----------+-----------+-----------+------------+\n",
       "|       14|     Cold|           1|         5|         M5|          3|    ACMAX22|   Hong Kong|\n",
       "|       -4|   Normal|           0|        11|        M11|         14|     AC1000|     Belgium|\n",
       "|        3|   Normal|           0|        10|        M10|         23|    ACMAX22|       China|\n",
       "|        2|   Normal|           0|         1|         M1|         25|     AC1000|         USA|\n",
       "|       -1|   Normal|           0|         2|         M2|         27|     FN39TG|      France|\n",
       "|       -8|   Normal|           1|         4|         M4|         17|     GG1919|     Finland|\n",
       "|       -1|   Normal|           0|        17|        M17|         11|     FN39TG|       Egypt|\n",
       "|        3|   Normal|           0|        18|        M18|         25|     JDNS77|   Indonesia|\n",
       "|       -4|   Normal|           0|        15|        M15|         19|    ACMAX22|      Israel|\n",
       "|        6|     Cold|           1|         3|         M3|         28|     JDNS77|      Brazil|\n",
       "|      -11|   Normal|           1|         4|         M4|         17|     GG1919|     Finland|\n",
       "|      -12|   Normal|           1|         2|         M2|         27|     FN39TG|      France|\n",
       "|        3|   Normal|           0|        16|        M16|         23|     AC1000|      Turkey|\n",
       "|        3|   Normal|           0|         9|         M9|         11|     GG1919|      Mexico|\n",
       "|       -8|   Normal|           1|        12|        M12|         26|     FN39TG|     Finland|\n",
       "|        3|   Normal|           0|        15|        M15|         19|    ACMAX22|      Israel|\n",
       "|       -7|   Normal|           1|         7|         M7|         13|     FN39TG|South Africa|\n",
       "|        1|   Normal|           0|         4|         M4|         17|     GG1919|     Finland|\n",
       "|        0|   Normal|           0|        17|        M17|         11|     FN39TG|       Egypt|\n",
       "|       -9|   Normal|           1|         4|         M4|         17|     GG1919|     Finland|\n",
       "+---------+---------+------------+----------+-----------+-----------+-----------+------------+\n",
       "only showing top 20 rows"
      ]
     },
     "execution_count": 16,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "select * from sensor_df\n",
    "\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "e08423d6",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------+---------------+\n",
      "|     country|temprange_count|\n",
      "+------------+---------------+\n",
      "|   Singapore|            134|\n",
      "|      Turkey|            144|\n",
      "|     Germany|            108|\n",
      "|      France|            160|\n",
      "|   Argentina|            132|\n",
      "|     Belgium|            112|\n",
      "|     Finland|            270|\n",
      "|       China|            137|\n",
      "|   Hong Kong|            143|\n",
      "|      Israel|            140|\n",
      "|         USA|            110|\n",
      "|      Mexico|            116|\n",
      "|   Indonesia|            132|\n",
      "|Saudi Arabia|            126|\n",
      "|      Canada|            152|\n",
      "|      Brazil|            136|\n",
      "|   Australia|            142|\n",
      "|       Egypt|            125|\n",
      "|South Africa|            152|\n",
      "+------------+---------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "\n",
    "\n",
    "select country,count(temprange) as temprange_count from sensor_df where temprange=\"Cold\"\n",
    "group by country \n",
    "\n",
    "\n",
    "\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "203c2b6e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+---------------+\n",
      "|country|temprange_count|\n",
      "+-------+---------------+\n",
      "+-------+---------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "\n",
    "\n",
    "select country,count(temprange) as temprange_count from sensor_df where temprange=\"Hot\"\n",
    "group by country\n",
    "\n",
    "\n",
    "\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "63455b9a",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "1d6f767a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-----------+\n",
      "|hvacproduct|\n",
      "+-----------+\n",
      "|    ACMAX22|\n",
      "|     AC1000|\n",
      "|     JDNS77|\n",
      "|     FN39TG|\n",
      "|     GG1919|\n",
      "+-----------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "dis_product=sensor_df.select('hvacproduct').distinct()\n",
    "dis_product.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "02697c22",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>hvacproduct</th><th>extreme Temp</th></tr>\n",
       "<tr><td>JDNS77</td><td>1603</td></tr>\n",
       "<tr><td>GG1919</td><td>1514</td></tr>\n",
       "<tr><td>FN39TG</td><td>1647</td></tr>\n",
       "<tr><td>ACMAX22</td><td>1638</td></tr>\n",
       "<tr><td>AC1000</td><td>1598</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+-----------+------------+\n",
       "|hvacproduct|extreme Temp|\n",
       "+-----------+------------+\n",
       "|     JDNS77|        1603|\n",
       "|     GG1919|        1514|\n",
       "|     FN39TG|        1647|\n",
       "|    ACMAX22|        1638|\n",
       "|     AC1000|        1598|\n",
       "+-----------+------------+"
      ]
     },
     "execution_count": 26,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "sensor_df.groupBy('hvacproduct').agg(count(col(\"extreme_temp\")).alias(\"extreme Temp\")).orderBy(col('hvacproduct').desc())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c158f5b2",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Pyspark 3",
   "language": "python",
   "name": "pyspark3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
