# Databricks notebook source
rides_df = spark.read.table('nycgreen_silver.green_cab_trips')

# COMMAND ----------

display(rides_df)

# COMMAND ----------

display(rides_df.groupBy('season').count())

# COMMAND ----------

rides_df = rides_df.dropna(how='any',subset=['season'])
display(rides_df.groupBy('season').count())

# COMMAND ----------

rides_df.select(F.year('lpep_pickup_datetime').alias('year')).distinct().display()

# COMMAND ----------

import pyspark.sql.functions as F
rides_2016 = rides_df.withColumn('year', F.year(F.col('lpep_pickup_datetime')))
rides_2016 = rides_2016.filter(F.col('year')==2016)
display(rides_2016)

# COMMAND ----------

rides_2016.count()

# COMMAND ----------


