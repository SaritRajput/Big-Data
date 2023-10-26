{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "98f088d3",
   "metadata": {},
   "outputs": [],
   "source": [
    "# # **Loading Hive Tables and Data Preparation for Analysis**\n",
    "\n",
    "import pyspark.sql.functions as F\n",
    "from pyspark.sql.window import Window\n",
    "from pyspark.sql.types import *\n",
    "from pyspark.sql.functions import * \n",
    "from pyspark.sql import SparkSession\n",
    "import pandas as pd\n",
    "import matplotlib.pyplot as plt"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "0b2f1190",
   "metadata": {},
   "outputs": [],
   "source": [
    "spark = SparkSession.builder.appName(\"Online Retail Analysis\").config(\n",
    "    \"spark.ui.port\", \"0\").config(\n",
    "        \"spark.sql.catalogImplementation=hive\").config(\n",
    "        \"spark.sql.warehouse.dir\",\n",
    "        \"hdfs://nameservice1/user/itv003722/warehouse/online_retail.db\").config(\n",
    "            \"spark.serializer\",\n",
    "    \"org.apache.spark.serializer.KryoSerializer\").enableHiveSupport().getOrCreate()\n",
    "spark.sparkContext.setLogLevel('OFF')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "c9234f28",
   "metadata": {},
   "outputs": [],
   "source": [
    "Rdf = spark.table('online_retail.online_retail')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "fc7cb7b4",
   "metadata": {},
   "outputs": [],
   "source": [
    "Rdf.createOrReplaceTempView('Rdf')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "1c71e38f",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+--------------------+--------+---------------+----------+-----------+--------------+\n",
      "|invoice_no|stock_code|         description|quantity|   invoice_date|unit_price|customer_id|       country|\n",
      "+----------+----------+--------------------+--------+---------------+----------+-----------+--------------+\n",
      "|     36365|    85123A|WHITE HANGING HEA...|       6|2010-12-01 8:26|      2.55|      17850|United Kingdom|\n",
      "|    536365|     71053| WHITE METAL LANTERN|       6|2010-12-01 8:26|      3.39|      17850|United Kingdom|\n",
      "|    536365|    84406B|CREAM CUPID HEART...|       8|2010-12-01 8:26|      2.75|      17850|United Kingdom|\n",
      "|    536365|    84029G|KNITTED UNION FLA...|       6|2010-12-01 8:26|      3.39|      17850|United Kingdom|\n",
      "|    536365|    84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 8:26|      3.39|      17850|United Kingdom|\n",
      "|    536365|     22752|SET 7 BABUSHKA NE...|       2|2010-12-01 8:26|      7.65|      17850|United Kingdom|\n",
      "|    536365|     21730|GLASS STAR FROSTE...|       6|2010-12-01 8:26|      4.25|      17850|United Kingdom|\n",
      "|    536366|     22633|HAND WARMER UNION...|       6|2010-12-01 8:28|      1.85|      17850|United Kingdom|\n",
      "|    536366|     22632|HAND WARMER RED P...|       6|2010-12-01 8:28|      1.85|      17850|United Kingdom|\n",
      "|    536367|     84879|ASSORTED COLOUR B...|      32|2010-12-01 8:34|      1.69|      13047|United Kingdom|\n",
      "|    536367|     22745|POPPY'S PLAYHOUSE...|       6|2010-12-01 8:34|       2.1|      13047|United Kingdom|\n",
      "|    536367|     22748|POPPY'S PLAYHOUSE...|       6|2010-12-01 8:34|       2.1|      13047|United Kingdom|\n",
      "|    536367|     22749|FELTCRAFT PRINCES...|       8|2010-12-01 8:34|      3.75|      13047|United Kingdom|\n",
      "|    536367|     22310|IVORY KNITTED MUG...|       6|2010-12-01 8:34|      1.65|      13047|United Kingdom|\n",
      "|    536367|     84969|BOX OF 6 ASSORTED...|       6|2010-12-01 8:34|      4.25|      13047|United Kingdom|\n",
      "|    536367|     22623|BOX OF VINTAGE JI...|       3|2010-12-01 8:34|      4.95|      13047|United Kingdom|\n",
      "|    536367|     22622|BOX OF VINTAGE AL...|       2|2010-12-01 8:34|      9.95|      13047|United Kingdom|\n",
      "|    536367|     21754|HOME BUILDING BLO...|       3|2010-12-01 8:34|      5.95|      13047|United Kingdom|\n",
      "|    536367|     21755|LOVE BUILDING BLO...|       3|2010-12-01 8:34|      5.95|      13047|United Kingdom|\n",
      "|    536367|     21777|RECIPE BOX WITH M...|       4|2010-12-01 8:34|      7.95|      13047|United Kingdom|\n",
      "+----------+----------+--------------------+--------+---------------+----------+-----------+--------------+\n",
      "only showing top 20 rows\n",
      "\n",
      "root\n",
      " |-- invoice_no: long (nullable = true)\n",
      " |-- stock_code: string (nullable = true)\n",
      " |-- description: string (nullable = true)\n",
      " |-- quantity: integer (nullable = true)\n",
      " |-- invoice_date: string (nullable = true)\n",
      " |-- unit_price: double (nullable = true)\n",
      " |-- customer_id: long (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "541909"
      ]
     },
     "execution_count": 35,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "Rdf.show()\n",
    "Rdf.printSchema()\n",
    "Rdf.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "b9b3e039",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th>summary</th>\n",
       "      <th>count</th>\n",
       "      <th>mean</th>\n",
       "      <th>stddev</th>\n",
       "      <th>min</th>\n",
       "      <th>max</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>invoice_no</th>\n",
       "      <td>532620</td>\n",
       "      <td>559964.7247099245</td>\n",
       "      <td>13447.583077573228</td>\n",
       "      <td>36365</td>\n",
       "      <td>581587</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>stock_code</th>\n",
       "      <td>541909</td>\n",
       "      <td>27623.240210938104</td>\n",
       "      <td>16799.737628427636</td>\n",
       "      <td>10002</td>\n",
       "      <td>m</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>description</th>\n",
       "      <td>541909</td>\n",
       "      <td>20713.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td></td>\n",
       "      <td>wrongly sold sets</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>quantity</th>\n",
       "      <td>541909</td>\n",
       "      <td>9.55224954743324</td>\n",
       "      <td>218.08115785023455</td>\n",
       "      <td>-80995</td>\n",
       "      <td>80995</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>invoice_date</th>\n",
       "      <td>541909</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>2010-12-01 10:03</td>\n",
       "      <td>2011-12-09 9:57</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>unit_price</th>\n",
       "      <td>541909</td>\n",
       "      <td>4.6111136260830365</td>\n",
       "      <td>96.75985306117936</td>\n",
       "      <td>-11062.06</td>\n",
       "      <td>38970.0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>customer_id</th>\n",
       "      <td>406829</td>\n",
       "      <td>15287.690570239585</td>\n",
       "      <td>1713.600303321604</td>\n",
       "      <td>12346</td>\n",
       "      <td>18287</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>country</th>\n",
       "      <td>541909</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>Australia</td>\n",
       "      <td>Unspecified</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "summary        count                mean              stddev  \\\n",
       "invoice_no    532620   559964.7247099245  13447.583077573228   \n",
       "stock_code    541909  27623.240210938104  16799.737628427636   \n",
       "description   541909             20713.0                 NaN   \n",
       "quantity      541909    9.55224954743324  218.08115785023455   \n",
       "invoice_date  541909                None                None   \n",
       "unit_price    541909  4.6111136260830365   96.75985306117936   \n",
       "customer_id   406829  15287.690570239585   1713.600303321604   \n",
       "country       541909                None                None   \n",
       "\n",
       "summary                    min                max  \n",
       "invoice_no               36365             581587  \n",
       "stock_code               10002                  m  \n",
       "description                     wrongly sold sets  \n",
       "quantity                -80995              80995  \n",
       "invoice_date  2010-12-01 10:03    2011-12-09 9:57  \n",
       "unit_price           -11062.06            38970.0  \n",
       "customer_id              12346              18287  \n",
       "country              Australia        Unspecified  "
      ]
     },
     "execution_count": 36,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#descritptive Statistics\n",
    "summary = Rdf.describe().toPandas()\n",
    "summary = summary.T\n",
    "summary.columns = summary.iloc[0]\n",
    "summary = summary.drop(summary.index[0])\n",
    "summary"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "id": "cbba4ad4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Data shape (rows, columns): 541909 x 8\n"
     ]
    }
   ],
   "source": [
    "def datashape(data):\n",
    "    print(\"Data shape (rows, columns):\", data.count(), \"x\", len(data.columns))\n",
    "    \n",
    "datashape(Rdf)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "id": "384ee1f0",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_no</th><th>stock_code</th><th>description</th><th>quantity</th><th>invoice_date</th><th>unit_price</th><th>customer_id</th><th>country</th></tr>\n",
       "<tr><td>9289</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>135080</td><td>0</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+----------+----------+-----------+--------+------------+----------+-----------+-------+\n",
       "|invoice_no|stock_code|description|quantity|invoice_date|unit_price|customer_id|country|\n",
       "+----------+----------+-----------+--------+------------+----------+-----------+-------+\n",
       "|      9289|         0|          0|       0|           0|         0|     135080|      0|\n",
       "+----------+----------+-----------+--------+------------+----------+-----------+-------+"
      ]
     },
     "execution_count": 40,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#check the missing value\n",
    "Rdf.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in Rdf.columns])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "id": "f772ef1a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------+\n",
      "|count(1)|\n",
      "+--------+\n",
      "|    9289|\n",
      "+--------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "\n",
    "select count(*) from Rdf where invoice_no is null\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "id": "11b0d591",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+--------------------+--------+----------------+----------+-----------+--------------+\n",
      "|invoice_no|stock_code|         description|quantity|    invoice_date|unit_price|customer_id|       country|\n",
      "+----------+----------+--------------------+--------+----------------+----------+-----------+--------------+\n",
      "|      null|     22556|PLASTERS IN TIN C...|     -12|2010-12-01 10:24|      1.65|      17548|United Kingdom|\n",
      "|      null|     21984|PACK OF 12 PINK P...|     -24|2010-12-01 10:24|      0.29|      17548|United Kingdom|\n",
      "|      null|     21983|PACK OF 12 BLUE P...|     -24|2010-12-01 10:24|      0.29|      17548|United Kingdom|\n",
      "|      null|     21980|PACK OF 12 RED RE...|     -24|2010-12-01 10:24|      0.29|      17548|United Kingdom|\n",
      "|      null|     21484|CHICK GREY HOT WA...|     -12|2010-12-01 10:24|      3.45|      17548|United Kingdom|\n",
      "|      null|     22557|PLASTERS IN TIN V...|     -12|2010-12-01 10:24|      1.65|      17548|United Kingdom|\n",
      "|      null|     22553|PLASTERS IN TIN S...|     -24|2010-12-01 10:24|      1.65|      17548|United Kingdom|\n",
      "|      null|     22960|JAM MAKING SET WI...|      -6|2010-12-01 12:38|      4.25|      17897|United Kingdom|\n",
      "|      null|     22632|HAND WARMER RED R...|      -1|2010-12-01 14:30|       2.1|      17841|United Kingdom|\n",
      "|      null|     22355|CHARLOTTE BAG SUK...|      -2|2010-12-01 14:30|      0.85|      17841|United Kingdom|\n",
      "+----------+----------+--------------------+--------+----------------+----------+-----------+--------------+\n",
      "only showing top 10 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "\n",
    "select * from Rdf where invoice_no is null\n",
    "\"\"\").show(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "id": "58a2b60e",
   "metadata": {},
   "outputs": [],
   "source": [
    "#remove the cancelled invoice start with C\n",
    "Rdf = Rdf[~Rdf['invoice_no'].startswith(\"C\")]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "id": "8ad32098",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Data shape (rows, columns): 532620 x 8\n"
     ]
    }
   ],
   "source": [
    "datashape(Rdf)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 45,
   "id": "311a7827",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+-----------------------------------+--------+---------------+----------+-----------+--------------+\n",
      "|invoice_no|stock_code|description                        |quantity|invoice_date   |unit_price|customer_id|country       |\n",
      "+----------+----------+-----------------------------------+--------+---------------+----------+-----------+--------------+\n",
      "|36365     |85123A    |WHITE HANGING HEART T-LIGHT HOLDER |6       |2010-12-01 8:26|2.55      |17850      |United Kingdom|\n",
      "|536365    |71053     |WHITE METAL LANTERN                |6       |2010-12-01 8:26|3.39      |17850      |United Kingdom|\n",
      "|536365    |84406B    |CREAM CUPID HEARTS COAT HANGER     |8       |2010-12-01 8:26|2.75      |17850      |United Kingdom|\n",
      "|536365    |84029G    |KNITTED UNION FLAG HOT WATER BOTTLE|6       |2010-12-01 8:26|3.39      |17850      |United Kingdom|\n",
      "|536365    |84029E    |RED WOOLLY HOTTIE WHITE HEART.     |6       |2010-12-01 8:26|3.39      |17850      |United Kingdom|\n",
      "|536365    |22752     |SET 7 BABUSHKA NESTING BOXES       |2       |2010-12-01 8:26|7.65      |17850      |United Kingdom|\n",
      "|536365    |21730     |GLASS STAR FROSTED T-LIGHT HOLDER  |6       |2010-12-01 8:26|4.25      |17850      |United Kingdom|\n",
      "|536366    |22633     |HAND WARMER UNION JACK             |6       |2010-12-01 8:28|1.85      |17850      |United Kingdom|\n",
      "|536366    |22632     |HAND WARMER RED POLKA DOT          |6       |2010-12-01 8:28|1.85      |17850      |United Kingdom|\n",
      "|536367    |84879     |ASSORTED COLOUR BIRD ORNAMENT      |32      |2010-12-01 8:34|1.69      |13047      |United Kingdom|\n",
      "|536367    |22745     |POPPY'S PLAYHOUSE BEDROOM          |6       |2010-12-01 8:34|2.1       |13047      |United Kingdom|\n",
      "|536367    |22748     |POPPY'S PLAYHOUSE KITCHEN          |6       |2010-12-01 8:34|2.1       |13047      |United Kingdom|\n",
      "|536367    |22749     |FELTCRAFT PRINCESS CHARLOTTE DOLL  |8       |2010-12-01 8:34|3.75      |13047      |United Kingdom|\n",
      "|536367    |22310     |IVORY KNITTED MUG COSY             |6       |2010-12-01 8:34|1.65      |13047      |United Kingdom|\n",
      "|536367    |84969     |BOX OF 6 ASSORTED COLOUR TEASPOONS |6       |2010-12-01 8:34|4.25      |13047      |United Kingdom|\n",
      "|536367    |22623     |BOX OF VINTAGE JIGSAW BLOCKS       |3       |2010-12-01 8:34|4.95      |13047      |United Kingdom|\n",
      "|536367    |22622     |BOX OF VINTAGE ALPHABET BLOCKS     |2       |2010-12-01 8:34|9.95      |13047      |United Kingdom|\n",
      "|536367    |21754     |HOME BUILDING BLOCK WORD           |3       |2010-12-01 8:34|5.95      |13047      |United Kingdom|\n",
      "|536367    |21755     |LOVE BUILDING BLOCK WORD           |3       |2010-12-01 8:34|5.95      |13047      |United Kingdom|\n",
      "|536367    |21777     |RECIPE BOX WITH METAL HEART        |4       |2010-12-01 8:34|7.95      |13047      |United Kingdom|\n",
      "+----------+----------+-----------------------------------+--------+---------------+----------+-----------+--------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#remove the row having null value o=in column invoice_no and cutomer_id that null id belongs to wholesaler sale\n",
    "Rdf.na.drop(subset=[\"invoice_no\"]) \\\n",
    "   .show(truncate=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 46,
   "id": "0db11576",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Data shape (rows, columns): 532620 x 8\n"
     ]
    }
   ],
   "source": [
    "datashape(Rdf)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 47,
   "id": "26208e02",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_no</th><th>stock_code</th><th>description</th><th>quantity</th><th>invoice_date</th><th>unit_price</th><th>customer_id</th><th>country</th></tr>\n",
       "<tr><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>134694</td><td>0</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+----------+----------+-----------+--------+------------+----------+-----------+-------+\n",
       "|invoice_no|stock_code|description|quantity|invoice_date|unit_price|customer_id|country|\n",
       "+----------+----------+-----------+--------+------------+----------+-----------+-------+\n",
       "|         0|         0|          0|       0|           0|         0|     134694|      0|\n",
       "+----------+----------+-----------+--------+------------+----------+-----------+-------+"
      ]
     },
     "execution_count": 47,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#check the missing value\n",
    "Rdf.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in Rdf.columns])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 48,
   "id": "90873c3c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+--------------------+--------+----------------+----------+-----------+--------------+\n",
      "|invoice_no|stock_code|         description|quantity|    invoice_date|unit_price|customer_id|       country|\n",
      "+----------+----------+--------------------+--------+----------------+----------+-----------+--------------+\n",
      "|    536414|     22139|                    |      56|2010-12-01 11:52|       0.0|       null|United Kingdom|\n",
      "|    536544|     21773|DECORATIVE ROSE B...|       1|2010-12-01 14:32|      2.51|       null|United Kingdom|\n",
      "|    536544|     21774|DECORATIVE CATS B...|       2|2010-12-01 14:32|      2.51|       null|United Kingdom|\n",
      "|    536544|     21786|  POLKADOT RAIN HAT |       4|2010-12-01 14:32|      0.85|       null|United Kingdom|\n",
      "|    536544|     21787|RAIN PONCHO RETRO...|       2|2010-12-01 14:32|      1.66|       null|United Kingdom|\n",
      "|    536544|     21790|  VINTAGE SNAP CARDS|       9|2010-12-01 14:32|      1.66|       null|United Kingdom|\n",
      "|    536544|     21791|VINTAGE HEADS AND...|       2|2010-12-01 14:32|      2.51|       null|United Kingdom|\n",
      "|    536544|     21801|CHRISTMAS TREE DE...|      10|2010-12-01 14:32|      0.43|       null|United Kingdom|\n",
      "|    536544|     21802|CHRISTMAS TREE HE...|       9|2010-12-01 14:32|      0.43|       null|United Kingdom|\n",
      "|    536544|     21803|CHRISTMAS TREE ST...|      11|2010-12-01 14:32|      0.43|       null|United Kingdom|\n",
      "+----------+----------+--------------------+--------+----------------+----------+-----------+--------------+\n",
      "only showing top 10 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "\n",
    "select * from Rdf where customer_id is null\n",
    "\"\"\").show(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 52,
   "id": "7e5c0d19",
   "metadata": {},
   "outputs": [],
   "source": [
    "#change the Datatype of quantity to Folat\n",
    "Rdf = Rdf.withColumn(\"quantity\", col(\"quantity\").cast(\"Float\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 53,
   "id": "60835004",
   "metadata": {},
   "outputs": [],
   "source": [
    "df=Rdf.withColumn('invoice_date', \\\n",
    "\n",
    "         to_timestamp('invoice_date').cast('timestamp'))\\"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 54,
   "id": "da8556ab",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- invoice_no: long (nullable = true)\n",
      " |-- stock_code: string (nullable = true)\n",
      " |-- description: string (nullable = true)\n",
      " |-- quantity: float (nullable = true)\n",
      " |-- invoice_date: timestamp (nullable = true)\n",
      " |-- unit_price: double (nullable = true)\n",
      " |-- customer_id: long (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 55,
   "id": "6ea6c80b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_date</th><th>count</th></tr>\n",
       "<tr><td>2010-12-01 08:26:00</td><td>7</td></tr>\n",
       "<tr><td>2010-12-01 08:28:00</td><td>2</td></tr>\n",
       "<tr><td>2010-12-01 08:34:00</td><td>16</td></tr>\n",
       "<tr><td>2010-12-01 08:35:00</td><td>1</td></tr>\n",
       "<tr><td>2010-12-01 08:45:00</td><td>20</td></tr>\n",
       "<tr><td>2010-12-01 09:00:00</td><td>1</td></tr>\n",
       "<tr><td>2010-12-01 09:01:00</td><td>2</td></tr>\n",
       "<tr><td>2010-12-01 09:02:00</td><td>16</td></tr>\n",
       "<tr><td>2010-12-01 09:09:00</td><td>1</td></tr>\n",
       "<tr><td>2010-12-01 09:32:00</td><td>18</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+-------------------+-----+\n",
       "|       invoice_date|count|\n",
       "+-------------------+-----+\n",
       "|2010-12-01 08:26:00|    7|\n",
       "|2010-12-01 08:28:00|    2|\n",
       "|2010-12-01 08:34:00|   16|\n",
       "|2010-12-01 08:35:00|    1|\n",
       "|2010-12-01 08:45:00|   20|\n",
       "|2010-12-01 09:00:00|    1|\n",
       "|2010-12-01 09:01:00|    2|\n",
       "|2010-12-01 09:02:00|   16|\n",
       "|2010-12-01 09:09:00|    1|\n",
       "|2010-12-01 09:32:00|   18|\n",
       "+-------------------+-----+"
      ]
     },
     "execution_count": 55,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Number i=of records as per date and Time\n",
    "df.groupby(\"invoice_date\").count().sort(\"invoice_date\", ascending=True).limit(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 56,
   "id": "28785e9a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------------------+\n",
      "|  min(invoice_date)|\n",
      "+-------------------+\n",
      "|2010-12-01 08:26:00|\n",
      "+-------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.select(min((df.invoice_date))).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 57,
   "id": "eea63af2",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------------------+\n",
      "|  max(invoice_date)|\n",
      "+-------------------+\n",
      "|2011-12-09 12:50:00|\n",
      "+-------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.select(max((df.invoice_date))).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "a8b4acf4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-----------------------+\n",
      "|count(DISTINCT country)|\n",
      "+-----------------------+\n",
      "|                     38|\n",
      "+-----------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#to check  total country\n",
    "spark.sql(\"\"\"\n",
    "\n",
    "select count(distinct country) from Rdf\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "72dc45e1",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------------+\n",
      "|           country|\n",
      "+------------------+\n",
      "|            Sweden|\n",
      "|         Singapore|\n",
      "|           Germany|\n",
      "|               RSA|\n",
      "|            France|\n",
      "|            Greece|\n",
      "|European Community|\n",
      "|           Belgium|\n",
      "|           Finland|\n",
      "|             Malta|\n",
      "|       Unspecified|\n",
      "|             Italy|\n",
      "|              EIRE|\n",
      "|         Lithuania|\n",
      "|            Norway|\n",
      "|             Spain|\n",
      "|           Denmark|\n",
      "|         Hong Kong|\n",
      "|           Iceland|\n",
      "|            Israel|\n",
      "+------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#distinct Country\n",
    "spark.sql(\"\"\"\n",
    "\n",
    "select distinct country from Rdf\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "7d866740",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_no</th><th>stock_code</th><th>description</th><th>quantity</th><th>invoice_date</th><th>unit_price</th><th>customer_id</th><th>country</th></tr>\n",
       "<tr><td>536540</td><td>22968</td><td>ROSE COTTAGE KEEP...</td><td>4.0</td><td>2010-12-01 14:05:00</td><td>9.95</td><td>14911</td><td>EIRE</td></tr>\n",
       "<tr><td>536540</td><td>85071A</td><td>BLUE CHARLIE+LOLA...</td><td>6.0</td><td>2010-12-01 14:05:00</td><td>2.95</td><td>14911</td><td>EIRE</td></tr>\n",
       "<tr><td>536540</td><td>85071C</td><td>&quot;CHARLIE+LOLA&quot;&quot;EX...</td><td>6.0</td><td>2010-12-01 14:05:00</td><td>2.55</td><td>14911</td><td>EIRE</td></tr>\n",
       "<tr><td>536540</td><td>22355</td><td>CHARLOTTE BAG SUK...</td><td>50.0</td><td>2010-12-01 14:05:00</td><td>0.85</td><td>14911</td><td>EIRE</td></tr>\n",
       "<tr><td>536540</td><td>21579</td><td>LOLITA  DESIGN  C...</td><td>6.0</td><td>2010-12-01 14:05:00</td><td>2.25</td><td>14911</td><td>EIRE</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+-------+\n",
       "|invoice_no|stock_code|         description|quantity|       invoice_date|unit_price|customer_id|country|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+-------+\n",
       "|    560894|     21381|MINI WOODEN HAPPY...|    12.0|2011-07-21 17:09:00|      1.69|      14911|   EIRE|\n",
       "|    560894|    47590B|PINK HAPPY BIRTHD...|    12.0|2011-07-21 17:09:00|      5.45|      14911|   EIRE|\n",
       "|    560894|    47590A|BLUE HAPPY BIRTHD...|    12.0|2011-07-21 17:09:00|      5.45|      14911|   EIRE|\n",
       "|    560894|     47566|       PARTY BUNTING|     4.0|2011-07-21 17:09:00|      4.95|      14911|   EIRE|\n",
       "|    560894|     23201|  JUMBO BAG ALPHABET|    20.0|2011-07-21 17:09:00|      2.08|      14911|   EIRE|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+-------+"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#to check  the country name EIRE\n",
    "df.filter(df.country == \"EIRE\").limit(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 58,
   "id": "9cb29312",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Replcae EIRE with Ireland\n",
    "df = df.replace(['EIRE'],['Ireland'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 59,
   "id": "8e7b855c",
   "metadata": {},
   "outputs": [],
   "source": [
    "#change the Datatype of quantity to Folat\n",
    "df = df.withColumn(\"quantity\", col(\"quantity\").cast(\"Float\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 60,
   "id": "78373b9e",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_no</th><th>stock_code</th><th>description</th><th>quantity</th><th>invoice_date</th><th>unit_price</th><th>customer_id</th><th>country</th></tr>\n",
       "<tr><td>36365</td><td>85123A</td><td>WHITE HANGING HEA...</td><td>6.0</td><td>2010-12-01 08:26:00</td><td>2.55</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536365</td><td>71053</td><td>WHITE METAL LANTERN</td><td>6.0</td><td>2010-12-01 08:26:00</td><td>3.39</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536365</td><td>84406B</td><td>CREAM CUPID HEART...</td><td>8.0</td><td>2010-12-01 08:26:00</td><td>2.75</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536365</td><td>84029G</td><td>KNITTED UNION FLA...</td><td>6.0</td><td>2010-12-01 08:26:00</td><td>3.39</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536365</td><td>84029E</td><td>RED WOOLLY HOTTIE...</td><td>6.0</td><td>2010-12-01 08:26:00</td><td>3.39</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536365</td><td>22752</td><td>SET 7 BABUSHKA NE...</td><td>2.0</td><td>2010-12-01 08:26:00</td><td>7.65</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536365</td><td>21730</td><td>GLASS STAR FROSTE...</td><td>6.0</td><td>2010-12-01 08:26:00</td><td>4.25</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536366</td><td>22633</td><td>HAND WARMER UNION...</td><td>6.0</td><td>2010-12-01 08:28:00</td><td>1.85</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536366</td><td>22632</td><td>HAND WARMER RED P...</td><td>6.0</td><td>2010-12-01 08:28:00</td><td>1.85</td><td>17850</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>84879</td><td>ASSORTED COLOUR B...</td><td>32.0</td><td>2010-12-01 08:34:00</td><td>1.69</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>22745</td><td>POPPY&#x27;S PLAYHOUSE...</td><td>6.0</td><td>2010-12-01 08:34:00</td><td>2.1</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>22748</td><td>POPPY&#x27;S PLAYHOUSE...</td><td>6.0</td><td>2010-12-01 08:34:00</td><td>2.1</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>22749</td><td>FELTCRAFT PRINCES...</td><td>8.0</td><td>2010-12-01 08:34:00</td><td>3.75</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>22310</td><td>IVORY KNITTED MUG...</td><td>6.0</td><td>2010-12-01 08:34:00</td><td>1.65</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>84969</td><td>BOX OF 6 ASSORTED...</td><td>6.0</td><td>2010-12-01 08:34:00</td><td>4.25</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>22623</td><td>BOX OF VINTAGE JI...</td><td>3.0</td><td>2010-12-01 08:34:00</td><td>4.95</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>22622</td><td>BOX OF VINTAGE AL...</td><td>2.0</td><td>2010-12-01 08:34:00</td><td>9.95</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>21754</td><td>HOME BUILDING BLO...</td><td>3.0</td><td>2010-12-01 08:34:00</td><td>5.95</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>21755</td><td>LOVE BUILDING BLO...</td><td>3.0</td><td>2010-12-01 08:34:00</td><td>5.95</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "<tr><td>536367</td><td>21777</td><td>RECIPE BOX WITH M...</td><td>4.0</td><td>2010-12-01 08:34:00</td><td>7.95</td><td>13047</td><td>United Kingdom</td></tr>\n",
       "</table>\n",
       "only showing top 20 rows\n"
      ],
      "text/plain": [
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+\n",
       "|invoice_no|stock_code|         description|quantity|       invoice_date|unit_price|customer_id|       country|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+\n",
       "|     36365|    85123A|WHITE HANGING HEA...|     6.0|2010-12-01 08:26:00|      2.55|      17850|United Kingdom|\n",
       "|    536365|     71053| WHITE METAL LANTERN|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|\n",
       "|    536365|    84406B|CREAM CUPID HEART...|     8.0|2010-12-01 08:26:00|      2.75|      17850|United Kingdom|\n",
       "|    536365|    84029G|KNITTED UNION FLA...|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|\n",
       "|    536365|    84029E|RED WOOLLY HOTTIE...|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|\n",
       "|    536365|     22752|SET 7 BABUSHKA NE...|     2.0|2010-12-01 08:26:00|      7.65|      17850|United Kingdom|\n",
       "|    536365|     21730|GLASS STAR FROSTE...|     6.0|2010-12-01 08:26:00|      4.25|      17850|United Kingdom|\n",
       "|    536366|     22633|HAND WARMER UNION...|     6.0|2010-12-01 08:28:00|      1.85|      17850|United Kingdom|\n",
       "|    536366|     22632|HAND WARMER RED P...|     6.0|2010-12-01 08:28:00|      1.85|      17850|United Kingdom|\n",
       "|    536367|     84879|ASSORTED COLOUR B...|    32.0|2010-12-01 08:34:00|      1.69|      13047|United Kingdom|\n",
       "|    536367|     22745|POPPY'S PLAYHOUSE...|     6.0|2010-12-01 08:34:00|       2.1|      13047|United Kingdom|\n",
       "|    536367|     22748|POPPY'S PLAYHOUSE...|     6.0|2010-12-01 08:34:00|       2.1|      13047|United Kingdom|\n",
       "|    536367|     22749|FELTCRAFT PRINCES...|     8.0|2010-12-01 08:34:00|      3.75|      13047|United Kingdom|\n",
       "|    536367|     22310|IVORY KNITTED MUG...|     6.0|2010-12-01 08:34:00|      1.65|      13047|United Kingdom|\n",
       "|    536367|     84969|BOX OF 6 ASSORTED...|     6.0|2010-12-01 08:34:00|      4.25|      13047|United Kingdom|\n",
       "|    536367|     22623|BOX OF VINTAGE JI...|     3.0|2010-12-01 08:34:00|      4.95|      13047|United Kingdom|\n",
       "|    536367|     22622|BOX OF VINTAGE AL...|     2.0|2010-12-01 08:34:00|      9.95|      13047|United Kingdom|\n",
       "|    536367|     21754|HOME BUILDING BLO...|     3.0|2010-12-01 08:34:00|      5.95|      13047|United Kingdom|\n",
       "|    536367|     21755|LOVE BUILDING BLO...|     3.0|2010-12-01 08:34:00|      5.95|      13047|United Kingdom|\n",
       "|    536367|     21777|RECIPE BOX WITH M...|     4.0|2010-12-01 08:34:00|      7.95|      13047|United Kingdom|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+\n",
       "only showing top 20 rows"
      ]
     },
     "execution_count": 60,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Daily Sales Activity\n",
    "df[(df[\"invoice_date\"]> '2010-12-01 08:00:00') & (df[\"invoice_date\"]< '2010-12-15 12:00:00') ]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 61,
   "id": "943dfede",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+\n",
      "|invoice_no|stock_code|         description|quantity|       invoice_date|unit_price|customer_id|       country|weekofyear|\n",
      "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+\n",
      "|     36365|    85123A|WHITE HANGING HEA...|     6.0|2010-12-01 08:26:00|      2.55|      17850|United Kingdom|        48|\n",
      "|    536365|     71053| WHITE METAL LANTERN|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|        48|\n",
      "|    536365|    84406B|CREAM CUPID HEART...|     8.0|2010-12-01 08:26:00|      2.75|      17850|United Kingdom|        48|\n",
      "|    536365|    84029G|KNITTED UNION FLA...|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|        48|\n",
      "|    536365|    84029E|RED WOOLLY HOTTIE...|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|        48|\n",
      "|    536365|     22752|SET 7 BABUSHKA NE...|     2.0|2010-12-01 08:26:00|      7.65|      17850|United Kingdom|        48|\n",
      "|    536365|     21730|GLASS STAR FROSTE...|     6.0|2010-12-01 08:26:00|      4.25|      17850|United Kingdom|        48|\n",
      "|    536366|     22633|HAND WARMER UNION...|     6.0|2010-12-01 08:28:00|      1.85|      17850|United Kingdom|        48|\n",
      "|    536366|     22632|HAND WARMER RED P...|     6.0|2010-12-01 08:28:00|      1.85|      17850|United Kingdom|        48|\n",
      "|    536367|     84879|ASSORTED COLOUR B...|    32.0|2010-12-01 08:34:00|      1.69|      13047|United Kingdom|        48|\n",
      "|    536367|     22745|POPPY'S PLAYHOUSE...|     6.0|2010-12-01 08:34:00|       2.1|      13047|United Kingdom|        48|\n",
      "|    536367|     22748|POPPY'S PLAYHOUSE...|     6.0|2010-12-01 08:34:00|       2.1|      13047|United Kingdom|        48|\n",
      "|    536367|     22749|FELTCRAFT PRINCES...|     8.0|2010-12-01 08:34:00|      3.75|      13047|United Kingdom|        48|\n",
      "|    536367|     22310|IVORY KNITTED MUG...|     6.0|2010-12-01 08:34:00|      1.65|      13047|United Kingdom|        48|\n",
      "|    536367|     84969|BOX OF 6 ASSORTED...|     6.0|2010-12-01 08:34:00|      4.25|      13047|United Kingdom|        48|\n",
      "|    536367|     22623|BOX OF VINTAGE JI...|     3.0|2010-12-01 08:34:00|      4.95|      13047|United Kingdom|        48|\n",
      "|    536367|     22622|BOX OF VINTAGE AL...|     2.0|2010-12-01 08:34:00|      9.95|      13047|United Kingdom|        48|\n",
      "|    536367|     21754|HOME BUILDING BLO...|     3.0|2010-12-01 08:34:00|      5.95|      13047|United Kingdom|        48|\n",
      "|    536367|     21755|LOVE BUILDING BLO...|     3.0|2010-12-01 08:34:00|      5.95|      13047|United Kingdom|        48|\n",
      "|    536367|     21777|RECIPE BOX WITH M...|     4.0|2010-12-01 08:34:00|      7.95|      13047|United Kingdom|        48|\n",
      "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#sale per week\n",
    "\n",
    "df = df.withColumn(\"weekofyear\", weekofyear(\"invoice_date\"))\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 62,
   "id": "b1ffa3b6",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_no</th><th>stock_code</th><th>description</th><th>quantity</th><th>invoice_date</th><th>unit_price</th><th>customer_id</th><th>country</th><th>weekofyear</th><th>amount</th></tr>\n",
       "<tr><td>560729</td><td>22705</td><td>WRAP GREEN PEARS </td><td>25.0</td><td>2011-07-20 14:39:00</td><td>0.42</td><td>17511</td><td>United Kingdom</td><td>29</td><td>10.5</td></tr>\n",
       "<tr><td>560729</td><td>22704</td><td>WRAP RED APPLES </td><td>25.0</td><td>2011-07-20 14:39:00</td><td>0.42</td><td>17511</td><td>United Kingdom</td><td>29</td><td>10.5</td></tr>\n",
       "<tr><td>560729</td><td>22710</td><td>WRAP I LOVE LONDON </td><td>25.0</td><td>2011-07-20 14:39:00</td><td>0.42</td><td>17511</td><td>United Kingdom</td><td>29</td><td>10.5</td></tr>\n",
       "<tr><td>560729</td><td>23241</td><td>TREASURE TIN GYMK...</td><td>6.0</td><td>2011-07-20 14:39:00</td><td>2.08</td><td>17511</td><td>United Kingdom</td><td>29</td><td>12.48</td></tr>\n",
       "<tr><td>560729</td><td>23242</td><td>TREASURE TIN BUFF...</td><td>6.0</td><td>2011-07-20 14:39:00</td><td>2.08</td><td>17511</td><td>United Kingdom</td><td>29</td><td>12.48</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+\n",
       "|invoice_no|stock_code|         description|quantity|       invoice_date|unit_price|customer_id|       country|weekofyear|amount|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+\n",
       "|    560729|     22705|   WRAP GREEN PEARS |    25.0|2011-07-20 14:39:00|      0.42|      17511|United Kingdom|        29|  10.5|\n",
       "|    560729|     22704|    WRAP RED APPLES |    25.0|2011-07-20 14:39:00|      0.42|      17511|United Kingdom|        29|  10.5|\n",
       "|    560729|     22710| WRAP I LOVE LONDON |    25.0|2011-07-20 14:39:00|      0.42|      17511|United Kingdom|        29|  10.5|\n",
       "|    560729|     23241|TREASURE TIN GYMK...|     6.0|2011-07-20 14:39:00|      2.08|      17511|United Kingdom|        29| 12.48|\n",
       "|    560729|     23242|TREASURE TIN BUFF...|     6.0|2011-07-20 14:39:00|      2.08|      17511|United Kingdom|        29| 12.48|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+"
      ]
     },
     "execution_count": 62,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Create a new column Amount to check the revenue per country\n",
    "df = df.withColumn(\"amount\", round(col(\"Quantity\") * col(\"unit_price\"),2))\n",
    "df.limit(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 63,
   "id": "d5fade60",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------+-------------+\n",
      "|       country|Total Revenue|\n",
      "+--------------+-------------+\n",
      "|United Kingdom|   9014127.87|\n",
      "|   Netherlands|    285446.34|\n",
      "|       Ireland|    283453.96|\n",
      "|       Germany|    228867.14|\n",
      "|        France|    209715.11|\n",
      "+--------------+-------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#Revenue Aggregate By top 5 countries\n",
    "df.groupBy(\"country\").agg(round(sum(\"amount\"),2).alias(\"Total Revenue\")).orderBy(col(\"Total Revenue\").desc()).show(5)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "a84075fb",
   "metadata": {},
   "source": [
    "It means United kingdom top country in case of Revenue"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 71,
   "id": "98ccd939",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------------------+----------+------------+\n",
      "|       invoice_date|invoice_no|Total Amount|\n",
      "+-------------------+----------+------------+\n",
      "|2010-12-01 08:26:00|     36365|        15.3|\n",
      "|2010-12-01 08:26:00|    536365|      123.82|\n",
      "|2010-12-01 08:28:00|    536366|        22.2|\n",
      "|2010-12-01 08:34:00|    536368|       70.05|\n",
      "|2010-12-01 08:34:00|    536367|      278.73|\n",
      "|2010-12-01 08:35:00|    536369|       17.85|\n",
      "|2010-12-01 08:45:00|    536370|      855.86|\n",
      "|2010-12-01 09:00:00|    536371|       204.0|\n",
      "|2010-12-01 09:01:00|    536372|        22.2|\n",
      "|2010-12-01 09:02:00|    536373|      259.86|\n",
      "|2010-12-01 09:09:00|    536374|       350.4|\n",
      "|2010-12-01 09:32:00|    536375|      259.86|\n",
      "|2010-12-01 09:32:00|    536376|       328.8|\n",
      "|2010-12-01 09:34:00|    536377|        22.2|\n",
      "|2010-12-01 09:37:00|    536378|      444.98|\n",
      "|2010-12-01 09:41:00|    536381|      449.98|\n",
      "|2010-12-01 09:41:00|    536379|       -27.5|\n",
      "|2010-12-01 09:41:00|    536380|        34.8|\n",
      "|2010-12-01 09:45:00|    536382|       430.6|\n",
      "|2010-12-01 09:49:00|    536383|       -4.65|\n",
      "+-------------------+----------+------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Daily sales Activity\n",
    "df.groupBy(\"invoice_date\",\"invoice_no\").agg(round(sum(\"amount\"),2).alias(\"Total Amount\")).orderBy(col(\"invoice_date\").asc()).show(20)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 72,
   "id": "4e3c18f7",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Check Hourly Sale \n",
    "df=df.withColumn(\"hour\",hour(\"invoice_date\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 73,
   "id": "b41b39de",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+----+\n",
      "|invoice_no|stock_code|         description|quantity|       invoice_date|unit_price|customer_id|       country|weekofyear|amount|hour|\n",
      "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+----+\n",
      "|     36365|    85123A|WHITE HANGING HEA...|     6.0|2010-12-01 08:26:00|      2.55|      17850|United Kingdom|        48|  15.3|   8|\n",
      "|    536365|     71053| WHITE METAL LANTERN|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|        48| 20.34|   8|\n",
      "|    536365|    84406B|CREAM CUPID HEART...|     8.0|2010-12-01 08:26:00|      2.75|      17850|United Kingdom|        48|  22.0|   8|\n",
      "|    536365|    84029G|KNITTED UNION FLA...|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|        48| 20.34|   8|\n",
      "|    536365|    84029E|RED WOOLLY HOTTIE...|     6.0|2010-12-01 08:26:00|      3.39|      17850|United Kingdom|        48| 20.34|   8|\n",
      "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+----+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 74,
   "id": "19122648",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----+-----------+------------+\n",
      "|hour|customer_id|Total Amount|\n",
      "+----+-----------+------------+\n",
      "|   6|      14305|        4.25|\n",
      "|   7|      12736|       234.0|\n",
      "|   7|      14619|      394.44|\n",
      "|   7|      12823|       535.5|\n",
      "|   7|      15694|       306.4|\n",
      "|   7|      14911|       539.0|\n",
      "|   7|      13090|       160.6|\n",
      "|   7|      15838|       277.5|\n",
      "|   7|      13098|     1974.06|\n",
      "|   7|      15189|      459.45|\n",
      "|   7|      16684|      1494.0|\n",
      "|   7|      16612|      317.36|\n",
      "|   7|      13026|      170.64|\n",
      "|   7|      15505|      880.58|\n",
      "|   7|      13741|      200.17|\n",
      "|   7|      16422|      385.14|\n",
      "|   7|      13953|       500.0|\n",
      "|   7|      12775|       419.8|\n",
      "|   7|      17679|      348.91|\n",
      "|   7|      18061|       213.8|\n",
      "+----+-----------+------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#sale per Hours\n",
    "df.groupBy(\"hour\",\"customer_id\").agg(round(sum(\"amount\"),2).alias(\"Total Amount\")).orderBy(col(\"hour\").asc()).show(20)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 75,
   "id": "00cf07d1",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table border='1'>\n",
       "<tr><th>invoice_no</th><th>stock_code</th><th>description</th><th>quantity</th><th>invoice_date</th><th>unit_price</th><th>customer_id</th><th>country</th><th>weekofyear</th><th>amount</th><th>hour</th></tr>\n",
       "<tr><td>36365</td><td>85123A</td><td>WHITE HANGING HEA...</td><td>6.0</td><td>2010-12-01 08:26:00</td><td>2.55</td><td>17850</td><td>United Kingdom</td><td>48</td><td>15.3</td><td>8</td></tr>\n",
       "<tr><td>536373</td><td>85123A</td><td>WHITE HANGING HEA...</td><td>6.0</td><td>2010-12-01 09:02:00</td><td>2.55</td><td>17850</td><td>United Kingdom</td><td>48</td><td>15.3</td><td>9</td></tr>\n",
       "<tr><td>536375</td><td>85123A</td><td>WHITE HANGING HEA...</td><td>6.0</td><td>2010-12-01 09:32:00</td><td>2.55</td><td>17850</td><td>United Kingdom</td><td>48</td><td>15.3</td><td>9</td></tr>\n",
       "<tr><td>536390</td><td>85123A</td><td>WHITE HANGING HEA...</td><td>64.0</td><td>2010-12-01 10:19:00</td><td>2.55</td><td>17511</td><td>United Kingdom</td><td>48</td><td>163.2</td><td>10</td></tr>\n",
       "<tr><td>536394</td><td>85123A</td><td>WHITE HANGING HEA...</td><td>32.0</td><td>2010-12-01 10:39:00</td><td>2.55</td><td>13408</td><td>United Kingdom</td><td>48</td><td>81.6</td><td>10</td></tr>\n",
       "</table>\n"
      ],
      "text/plain": [
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+----+\n",
       "|invoice_no|stock_code|         description|quantity|       invoice_date|unit_price|customer_id|       country|weekofyear|amount|hour|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+----+\n",
       "|     36365|    85123A|WHITE HANGING HEA...|     6.0|2010-12-01 08:26:00|      2.55|      17850|United Kingdom|        48|  15.3|   8|\n",
       "|    536373|    85123A|WHITE HANGING HEA...|     6.0|2010-12-01 09:02:00|      2.55|      17850|United Kingdom|        48|  15.3|   9|\n",
       "|    536375|    85123A|WHITE HANGING HEA...|     6.0|2010-12-01 09:32:00|      2.55|      17850|United Kingdom|        48|  15.3|   9|\n",
       "|    536390|    85123A|WHITE HANGING HEA...|    64.0|2010-12-01 10:19:00|      2.55|      17511|United Kingdom|        48| 163.2|  10|\n",
       "|    536394|    85123A|WHITE HANGING HEA...|    32.0|2010-12-01 10:39:00|      2.55|      13408|United Kingdom|        48|  81.6|  10|\n",
       "+----------+----------+--------------------+--------+-------------------+----------+-----------+--------------+----------+------+----+"
      ]
     },
     "execution_count": 75,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#check the sale as per stock-code\n",
    "df.filter(df.stock_code == \"85123A\").limit(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 76,
   "id": "05c7b6e5",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+-----------+\n",
      "|invoice_no|Basket Size|\n",
      "+----------+-----------+\n",
      "|    560996|        101|\n",
      "|    561032|          6|\n",
      "|    561473|         25|\n",
      "|    562162|         14|\n",
      "|    562933|        290|\n",
      "|    563540|         30|\n",
      "|    564921|          1|\n",
      "|    565456|         50|\n",
      "|    565556|          1|\n",
      "|    565735|          1|\n",
      "|    565750|         10|\n",
      "|    566486|         13|\n",
      "|    566571|         75|\n",
      "|    566924|          1|\n",
      "|    567201|          1|\n",
      "|    567816|         21|\n",
      "|    568387|         22|\n",
      "|    568402|         14|\n",
      "|    568706|         17|\n",
      "|    568715|         37|\n",
      "+----------+-----------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#No. of transaction per invoice\n",
    "df.groupBy(\"invoice_no\").agg(count(\"stock_code\").alias(\"Basket Size\")).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 77,
   "id": "67f069cf",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+--------------------+-------------------+\n",
      "|invoice_no|         Description|Total Qunatity sold|\n",
      "+----------+--------------------+-------------------+\n",
      "|    581483|\"PAPER CRAFT , LI...|            80995.0|\n",
      "|    541431|MEDIUM CERAMIC TO...|            74215.0|\n",
      "|    578841|ASSTD DESIGN 3D P...|            12540.0|\n",
      "|    542504|                    |             5568.0|\n",
      "|    573008|WORLD WAR 2 GLIDE...|             4800.0|\n",
      "+----------+--------------------+-------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#Total Item sold by frequency\n",
    "df.groupBy(\"invoice_no\",\"Description\").agg(sum(\"quantity\").alias(\"Total Qunatity sold\")).orderBy(col(\"Total Qunatity sold\").desc()).show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "1e40ef5b",
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
