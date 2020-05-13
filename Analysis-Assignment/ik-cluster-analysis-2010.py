from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



# spark = SparkSession.builder.master("local").appName("Demo Spark Python Cluster Program").getOrCreate()
spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df2 = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/2000-2018.txt")#.limit(1000)
# df2 = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/20.txt")#.limit(1000)

df2 = df2.withColumn('Air Temperature', df2['value'].substr(88, 5).cast('float') /10) \
.withColumn('Atmospheric Pressure', df2['value'].substr(100, 5).cast('float')/ 10) \
.drop('value').filter("year == 2010")
# df2.show(10)
# HDFS copy
df2.write.csv('hdfs://namenode/output/itmd-521/ik/2010')

# Local copy
df2.toPandas().to_csv('weather.csv')
