# In Python Page 228 of E-book
from __future__ import print_function

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()
#shuffle partition 
spark.conf.set("spark.sql.shuffle.partitions",20)
#reading 2001 file
df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://namenode/output/itmd-521/tdp/2001/valid-records-temperature")
#creating a temporary table
df.registerTempTable('Table')
#query related to selection of air temperature by range 
df1=spark.sql('SELECT month(Observation_Date) as Month,Min(Air_Temperature) as Min,Max(Air_Temperature) as Max FROM Table where Air_Temperature between -73 and 46 group by month(Observation_Date) order by month(Observation_Date)')
#writing the file
df1.write.csv(path="hdfs://namenode/output/itmd-521/tdp/2001/Min_Max-using 20 shuffle partition", mode="overwrite",header="true")