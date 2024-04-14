# Databricks notebook source
rides_df = spark.read.table('nycdata_silver.yellow_cab_trips')

# COMMAND ----------

display(rides_df)

# COMMAND ----------

display(rides_df.groupBy('season').count())

# COMMAND ----------

display(rides_df.groupBy('pick_up_of_the_week').count())

# COMMAND ----------

import pyspark.sql.functions as F
rides_2018 = rides_df.withColumn('year', F.year(F.col('pickup_datetime')))
rides_2018 = rides_2018.filter(F.col('year')==2018)
display(rides_2018)

# COMMAND ----------

rides_2018.count()

# COMMAND ----------


