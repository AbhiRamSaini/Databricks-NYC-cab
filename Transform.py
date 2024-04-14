# Databricks notebook source
payments_df = spark.read.table("nycdata_bronze.payment_ref")
rates_df = spark.read.table("nycdata_bronze.rate_code_ref")
yellow_df = spark.sql("select * from nycdata_bronze.yellow_cab_trip")

# COMMAND ----------

# Display count of each distinct vendor_id
display(yellow_df.groupBy('vendor_id').count())

# COMMAND ----------

display(yellow_df.select("total_amount").summary("count","min","25%","75%","max"))


# COMMAND ----------

display(yellow_df.select("trip_distance").summary("count","min","25%","75%","max"))


# COMMAND ----------

display(yellow_df.select("passenger_count").summary("count","min","25%", "75%", "max"))


# COMMAND ----------

import pyspark.sql.functions as F
clean_yellow_df = yellow_df.filter((F.col("passenger_count")>1) & (F.col("passenger_count")<6))


# COMMAND ----------

clean_yellow_df = clean_yellow_df.filter((F.col("trip_distance")>=1)& (F.col("trip_distance")<=1000))


# COMMAND ----------

clean_yellow_df.count()


# COMMAND ----------

clean_yellow_df.createOrReplaceTempView("clean_yellow_df")
clean_amt_sql = "select * from clean_yellow_df where total_amount > 0 and total_amount < 1000 "
clean_yellow_df = spark.sql(clean_amt_sql) 

# COMMAND ----------

clean_yellow_df.count()


# COMMAND ----------

display(clean_yellow_df)


# COMMAND ----------

clean_rate_code_sql = """select * from clean_yellow_df where rate_code is not null and vendor_id is not null and store_and_fwd_flag is not null"""
clean_yellow_df = spark.sql(clean_rate_code_sql)

# COMMAND ----------

clean_yellow_df.count()


# COMMAND ----------

display(clean_yellow_df)


# COMMAND ----------

display(clean_yellow_df.filter(F.col("total_amount").isNotNull()).filter(F.col("total_amount")>0))


# COMMAND ----------

clean_yellow_df.count()


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from clean_yellow_df

# COMMAND ----------

clean_yellow_df.count()


# COMMAND ----------

display(clean_yellow_df)


# COMMAND ----------

display(clean_yellow_df.filter(F.col("total_amount").isNotNull()).filter(F.col("total_amount")>0))


# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists nycdata_silver

# COMMAND ----------

enriched_yellow_df = (clean_yellow_df.withColumn("pick_up_of_the_week",F.date_format(F.col('pickup_datetime'),'E'))
                      .withColumn('drop_off_day_of_the_week',F.date_format(F.col('dropoff_datetime'),'E'))
                      .withColumn('hour_of_day',F.hour(F.col('pickup_datetime')))
                      .withColumn('peak_non_peak',F.expr("Case when hour_of_day < 19 then 'peak' else 'non-peak' end"))
                      .withColumn('month_of_year',F.month(F.col('pickup_datetime')))
                      .withColumn('season',F.when(((F.col('month_of_year')>4)&(F.col('month_of_year')<10)),'Summer').otherwise('Winter'))
                      )

# COMMAND ----------

display(enriched_yellow_df)


# COMMAND ----------

enriched_yellow_df.count()


# COMMAND ----------

display(rates_df)

# COMMAND ----------

yellow_df_with_ratings = enriched_yellow_df.join(rates_df, enriched_yellow_df.rate_code == rates_df.rate_code_id, 'leftouter')
display(yellow_df_with_ratings)

# COMMAND ----------

yellow_df_with_ratings.count()


# COMMAND ----------

display(yellow_df_with_ratings.select('payment_type').distinct())


# COMMAND ----------

yellow_df_with_payments = yellow_df_with_ratings.join(payments_df, ['payment_type'],"outer")


# COMMAND ----------

yellow_df_with_payments.count()


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS nycdata_silver.yellow_cab_trips")
yellow_df_with_payments.drop("payment_desc").write.mode("overwrite").saveAsTable("nycdata_silver.yellow_cab_trips")


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS nycdata_silver.yellow_cab_trips")
yellow_df_with_payments.withColumnRenamed("payment_desc", "n_payment_desc").write.mode("overwrite").saveAsTable("nycdata_silver.yellow_cab_trips")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nycdata_silver.yellow_cab_trips
