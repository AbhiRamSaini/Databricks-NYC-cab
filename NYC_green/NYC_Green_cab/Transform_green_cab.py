# Databricks notebook source
payments_df = spark.read.table("nycgreen_bronze.payment_ref")
rates_df = spark.read.table("nycgreen_bronze.rate_code_ref")
green_df = spark.sql("select * from nycgreen_bronze.green_cab_trip")

# COMMAND ----------

display(green_df)

# COMMAND ----------

display(green_df.groupBy('VendorID').count())

# COMMAND ----------

display(green_df.select("Total_amount").summary("count","min","25%","75%","max"))

# COMMAND ----------

display(green_df.select("trip_distance").summary("count","min","25%","75%","max"))

# COMMAND ----------

display(green_df.select("passenger_count").summary("count","min","25%", "75%", "max"))

# COMMAND ----------

import pyspark.sql.functions as F
clean_green_df = green_df.filter((F.col("passenger_count")>=1) & (F.col("passenger_count")<=6))

# COMMAND ----------

clean_green_df = clean_green_df.filter((F.col("trip_distance")>=1)& (F.col("trip_distance")<=1000))

# COMMAND ----------

clean_green_df.count()

# COMMAND ----------

clean_green_df = clean_green_df.filter((F.col('total_amount')>0)&(F.col('total_amount')<1000))

# COMMAND ----------

clean_green_df.count()

# COMMAND ----------

display(clean_green_df)

# COMMAND ----------

clean_green_df.select('vendorid').distinct().display()
clean_green_df.select('trip_type_').distinct().display()
clean_green_df.select('payment_type').distinct().display()
clean_green_df.select('ratecodeid').distinct().display()
clean_green_df.select('store_and_fwd_flag').distinct().display()

# COMMAND ----------

clean_green_df.createOrReplaceTempView("clean_green_df")

# COMMAND ----------

clean_rate_code_sql = """select * from clean_green_df where payment_type is not null and trip_type_ is not null"""
clean_green_df = spark.sql(clean_rate_code_sql)

# COMMAND ----------

clean_green_df.count()

# COMMAND ----------

display(clean_green_df)

# COMMAND ----------

clean_green_df = clean_green_df.drop("store_and_fwd_flag")

# COMMAND ----------

display(clean_green_df)

# COMMAND ----------

clean_green_df.select('Ehail_fee').distinct().display()

# COMMAND ----------

clean_green_df = clean_green_df.drop("Ehail_fee")
display(clean_green_df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists nycgreen_silver

# COMMAND ----------

enriched_green_df = (clean_green_df.withColumn("pick_up_of_the_week",F.date_format(F.col('lpep_pickup_datetime'),'E'))
                      .withColumn('drop_off_day_of_the_week',F.date_format(F.col('Lpep_dropoff_datetime'),'E'))
                      .withColumn('hour_of_day',F.hour(F.col('lpep_pickup_datetime')))
                      .withColumn('peak_non_peak',F.expr("Case when hour_of_day < 19 then 'peak' else 'non-peak' end"))
                      .withColumn('month_of_year',F.month(F.col('lpep_pickup_datetime')))
                      .withColumn('season',F.when(((F.col('month_of_year')>4)&(F.col('month_of_year')<10)),'Summer').otherwise('Winter'))
                      )

# COMMAND ----------

display(enriched_green_df)

# COMMAND ----------

enriched_green_df.count()

# COMMAND ----------

display(rates_df)

# COMMAND ----------

green_df_ratings = enriched_green_df.join(rates_df,enriched_green_df.RateCodeID == rates_df.RateCodeID, 'leftouter')
display(green_df_ratings)

# COMMAND ----------

green_df_ratings.select('ratecodedesc').distinct().display()

# COMMAND ----------

green_df_ratings.count()

# COMMAND ----------

display(green_df_ratings.select('payment_type').distinct())

# COMMAND ----------

green_df_with_payments = green_df_ratings.join(payments_df, ['payment_type'],'outer')

# COMMAND ----------

green_df_with_payments.count()

# COMMAND ----------

spark.sql("drop table if exists nycgreen.green_cab_trips")

# COMMAND ----------

spark.sql("drop table if exists nycgreen.green_cab_trips")
# Drop the column ratecodeid from the dataframe
green_df_with_payments = green_df_with_payments.drop("ratecodeid").drop("new_ratecodeid")

# Save the table with the renamed column
green_df_with_payments.write.mode("overwrite").saveAsTable("nycgreen_silver.green_cab_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nycgreen_silver.green_cab_trips

# COMMAND ----------


