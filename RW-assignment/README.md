# tpatil3
Private repo for ITM
# Tanmay Patil

# ITMD-521 Read & Write 

## Cluster Command

```bash
#parquet 
spark-submit --verbose --name tdp-parquet-file-read-write.py --master yarn --deploy-mode cluster --num-executors 11 tdp-parquet-file-read-write.py 
#csv
spark-submit --verbose --name tdp-csv-file-read-write.py --master yarn --deploy-mode cluster --num-executors 11 tdp-csv-file-read-write.py 
```

### Your Explanation

# selecting data for 2001 year from the given data
# Referred Chapter 7- PG. NO 125-150
df3.createOrReplaceTempView("MainTable") 
Creating temporary view 
# Referred Chapter 10- PG. NO 188
df5_2001 = spark.sql("SELECT * FROM MainTable WHERE year(Observation_Date) = 2001")
Using sql query we are selecting year from the observation table where year is 2001 from cluster

# Dealing with bad/corrupt data 
We would be using an SQL query which will filter out all the indexes of the values containing value '99999' and create an array of it. This array will then be replaced by the mean value of that particular column.
Second type of Bad Data would be handled by trimming the data as per the schema and removing any kind of special characters present in the data so that all the values in that column are of same length.

# Dealing with optimize partitions using executors and memory

We can use number of executors to fasten the execution time by   " --num-executors NUM " by default it is NUM we can assign it with how many you want like I want executors to be 11 we will use " --num-executors 11 ". Same we can use for executor memory. For partitions we can use partionby() or repartition() for optimization. Can use compression such as lz4 for compression.
