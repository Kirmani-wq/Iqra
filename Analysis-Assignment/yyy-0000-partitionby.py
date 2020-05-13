from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import pip



# spark = SparkSession.builder.master("local").appName("Demo Spark Python Cluster Program").getOrCreate()
spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df2 = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/2000-2018.txt")#.limit(1000)
# df2 = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/20.txt")#.limit(1000)

df2 = df2.withColumn('year',year(to_date(df2['value'].substr(16,8), 'yyyyMMdd'))) \
.drop('value').filter("year == 2010")
partMonth=df2.partition("month")

df2.write.csv('hdfs://namenode/output/itmd-521/ik/2010')

# Local copy
df2.toPandas().to_csv('weather.csv')
print(df2)