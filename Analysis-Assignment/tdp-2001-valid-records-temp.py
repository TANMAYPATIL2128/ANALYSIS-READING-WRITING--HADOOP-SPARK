# In Python Page 228 of E-book
from __future__ import print_function

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

#reading 2001 file
df2 = spark.read.format("csv").option("header", "true").load("hdfs://namenode/output/itmd-521/tdp/2001/csv-file")
#importing functions 
from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
#creating a dataframe
df3 = df2.withColumn('Weather_Station', df2['value'].substr(5, 6))\
.withColumn('WBAN', df2['value'].substr(11, 5))\
.withColumn('Observation_Date',to_date(df2['value'].substr(16,8),"yyyyMMdd"))\
.withColumn('Observation_Hour', df2['value'].substr(24, 4).cast(IntegerType()))\
.withColumn('Latitude', df2['value'].substr(29, 6).cast('float') / 1000)\
.withColumn('Longitude', df2['value'].substr(35, 7).cast('float') / 1000)\
.withColumn('Elevation', df2['value'].substr(47, 5).cast(IntegerType()))\
.withColumn('Wind_Direction', df2['value'].substr(61, 3).cast(IntegerType()))\
.withColumn('WD_Quality_Code', df2['value'].substr(64, 1).cast(IntegerType()))\
.withColumn('Sky_Ceiling_Height', df2['value'].substr(71, 5).cast(IntegerType()))\
.withColumn('SC_Quality_Code', df2['value'].substr(76, 1).cast(IntegerType()))\
.withColumn('Visibility_Distance', df2['value'].substr(79, 6).cast(IntegerType()))\
.withColumn('VD_Quality_Code', df2['value'].substr(86, 1).cast(IntegerType()))\
.withColumn('Air_Temperature', df2['value'].substr(88, 5).cast('float') /10)\
.withColumn('AT_Quality_Code', df2['value'].substr(93, 1).cast(IntegerType()))\
.withColumn('Dew_Point', df2['value'].substr(94, 5).cast('float'))\
.withColumn('DP_Quality_Code', df2['value'].substr(99, 1).cast(IntegerType()))\
.withColumn('Atmospheric_Pressure', df2['value'].substr(100, 5).cast('float')/ 10)\
.withColumn('AP_Quality_Code', df2['value'].substr(105, 1).cast(IntegerType()))
df3.show(10)

#filtering the air temperature data in which there's no 999.9
df_AT_NoBadRecords=df3.filter(col("Air_Temperature") != 999.9)

#writng the file
df_AT_NoBadRecords.write.format("csv").mode("overwrite").option("header","true").save("hdfs://namenode/output/itmd-521/tdp/2001/valid-records-temperature")

