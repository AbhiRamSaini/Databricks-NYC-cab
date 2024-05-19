# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tripdata/green/

# COMMAND ----------

green_df = spark.read.option("Header",True).option('inferschema',True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/green/")
display(green_df)

# COMMAND ----------

green_df.count()

# COMMAND ----------

green_df.printSchema()

# COMMAND ----------

payment_type_df = spark.read.option("Header", True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")
display(payment_type_df)

# COMMAND ----------

rate_code_df = spark.read.option("Header",True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")
display(rate_code_df)

# COMMAND ----------

zone_df = spark.read.option("Header",True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv")
display(zone_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fixing schemas

# COMMAND ----------

green_df.display()
green_df.printSchema()

# COMMAND ----------

green_df.select('Ehail_fee').distinct().display()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, StringType, IntegerType, DoubleType

green_df = green_df\
    .withColumn("VendorId", F.col("VendorId").cast(IntegerType()))\
    .withColumn("lpep_pickup_datetime", F.col("lpep_pickup_datetime").cast(TimestampType()))\
    .withColumn("Lpep_dropoff_datetime", F.col("Lpep_dropoff_datetime").cast(TimestampType()))\
    .withColumn("Passenger_count", F.col("passenger_count").cast(IntegerType()))\
    .withColumn("Trip_distance", F.col("Trip_distance").cast(DoubleType()))\
    .withColumn("Pickup_longitude", F.col("Pickup_longitude").cast(DoubleType()))\
    .withColumn("Pickup_latitude", F.col("Pickup_latitude").cast(DoubleType()))\
    .withColumn("RateCodeID", F.col("RateCodeID").cast(IntegerType()))\
    .withColumn("Store_and_fwd_flag", F.col("store_and_fwd_flag").cast(IntegerType()))\
    .withColumn("Dropoff_longitude", F.col("Dropoff_longitude").cast(DoubleType()))\
    .withColumn("Dropoff_latitude", F.col("Dropoff_latitude").cast(DoubleType()))\
    .withColumn("Payment_type", F.col("Payment_type").cast(StringType()))\
    .withColumn("Fare_amount", F.col("Fare_amount").cast(DoubleType()))\
    .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))\
    .withColumn("Mta_tax", F.col("Mta_tax").cast(DoubleType()))\
    .withColumn("Tip_amount", F.col("Tip_amount").cast(DoubleType()))\
    .withColumn("Tolls_amount", F.col("Tolls_amount").cast(DoubleType()))\
    .withColumn("Total_amount", F.col("Total_amount").cast(DoubleType()))\
    .withColumn("Trip_distance", F.col("Trip_distance").cast(DoubleType()))\
    .withColumn("Extra", F.col("Extra").cast(DoubleType()))\
    .withColumn("Ehail_fee", F.col("Ehail_fee").cast(DoubleType()))


# COMMAND ----------

green_df.printSchema()

# COMMAND ----------

green_df.select('Payment_type').distinct().display()

# COMMAND ----------

zone_df = zone_df.withColumn("LocationID", F.col("LocationID").cast(IntegerType()))

# COMMAND ----------

payment_type_df = payment_type_df.withColumn("payment_type", F.col("payment_type").cast(IntegerType()))

# COMMAND ----------

rate_code_df = rate_code_df.withColumn('RateCodeID', F.col('RateCodeID').cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC '''Schema creaction'''

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists nycgreen_bronze

# COMMAND ----------

payment_type_df.write.mode("overwrite").saveAsTable("nycgreen_bronze.payment_ref")

# COMMAND ----------

# DBTITLE 1,green bronze payment reference
# MAGIC %sql select * from nycgreen_bronze.payment_ref

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table nycgreen_bronze.rate_code_ref

# COMMAND ----------

rate_code_df.write.mode("overwrite").saveAsTable("nycgreen_bronze.rate_code_ref")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from nycgreen_bronze.rate_code_ref

# COMMAND ----------

new_column_names = [column.replace(' ', '_').replace(',', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for column in green_df.columns]
green_df = green_df.toDF(*new_column_names)

green_df.write.mode("overwrite").saveAsTable('nycgreen_bronze.green_cab_trip')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from nycgreen_bronze.green_cab_trip

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended nycgreen_bronze.green_cab_trip

# COMMAND ----------


