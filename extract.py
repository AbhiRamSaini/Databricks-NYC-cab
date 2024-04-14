# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/nyctaxi/readme_nyctaxi.txt

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/taxizone/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/taxizone/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/reference/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tables/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow

# COMMAND ----------

yellow_delta_df = spark.read.format("delta").load("dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")
display(yellow_delta_df)

# COMMAND ----------

fhv_df = spark.read.option("Header", True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/fhv")
display(fhv_df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tripdata/green

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

green_df = spark.read.option("Header", True).option("inferschema", True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/green")
display(green_df)

# COMMAND ----------

green_df.count()

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tripdata/

# COMMAND ----------

yellow_df = spark.read.option("Header",True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")

# COMMAND ----------

display(yellow_df)

# COMMAND ----------

yellow_df.printSchema()

# COMMAND ----------

yellow_df.count()

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/taxizone/

# COMMAND ----------

payment_type_df = spark.read.option("Header", True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")
display(payment_type_df)

# COMMAND ----------

payment_type_df.printSchema()

# COMMAND ----------

rate_code_df = spark.read.option("Header",True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")
display(rate_code_df)

# COMMAND ----------

rate_code_df.printSchema()

# COMMAND ----------

zone_df = spark.read.option("Header",True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv")
display(zone_df)

# COMMAND ----------

zone_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Fix schema

# COMMAND ----------

yellow_df.display()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, StringType, IntegerType, DoubleType
yellow_df = yellow_df\
    .withColumn("vendor_id", F.col("vendor_id").cast(IntegerType()))\
    .withColumn("pickup_datetime", F.col("pickup_datetime").cast(TimestampType()))\
    .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast(TimestampType()))\
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))\
    .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))\
    .withColumn("pickup_longitude", F.col("pickup_longitude").cast(DoubleType()))\
    .withColumn("pickup_latitude", F.col("pickup_latitude").cast(DoubleType()))\
    .withColumn("rate_code", F.col("rate_code").cast(IntegerType()))\
    .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(IntegerType()))\
    .withColumn("dropoff_longitude", F.col("dropoff_longitude").cast(DoubleType()))\
    .withColumn("dropoff_latitude", F.col("dropoff_latitude").cast(DoubleType()))\
    .withColumn("payment_type", F.col("payment_type").cast(StringType()))\
    .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))\
    .withColumn("surcharge", F.col("surcharge").cast(DoubleType()))\
    .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))\
    .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))\
    .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))\
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))


yellow_df.printSchema()

# COMMAND ----------

yellow_df.select("payment_type").distinct().display()

# COMMAND ----------

zone_df = zone_df.withColumn("LocationID", F.col("LocationID").cast(IntegerType()))

# COMMAND ----------

payment_type_df.printSchema()
payment_type_df.display()

# COMMAND ----------

payment_type_df = payment_type_df.withColumn("payment_type", F.col("payment_type").cast(IntegerType()))

# COMMAND ----------

payment_type_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists nycdata_bronze

# COMMAND ----------

payment_type_df.write.mode("overwrite").saveAsTable("nycdata_bronze.payment_ref")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nycdata_bronze.payment_ref

# COMMAND ----------

rate_code_df = rate_code_df.withColumnRenamed("RateCodeID","rate_code_id").withColumnRenamed("RateCodeDesc","rate_code_desc")

# COMMAND ----------

rate_code_df.display()

# COMMAND ----------

from pyspark.sql.types import IntegerType
rate_code_df = rate_code_df.withColumn('rate_code_id',F.col('rate_code_id').cast(IntegerType()))

# COMMAND ----------

rate_code_df.write.mode("overwrite").saveAsTable("nycdata_bronze.rate_code_ref")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from nycdata_bronze.rate_code_ref

# COMMAND ----------

yellow_df.write.mode("overwrite").saveAsTable("nycdata_bronze.yellow_cab_trip")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe nycdata_bronze.yellow_cab_trip

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended nycdata_bronze.yellow_cab_trip
