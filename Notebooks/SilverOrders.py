# Databricks notebook source
# MAGIC %md Dataload

# COMMAND ----------

df=spark.read.format("parquet")\
    .option("header", "true")\
    .load("abfss://bronze@databricksretailsa.dfs.core.windows.net/Orders")


# COMMAND ----------

from pyspark.sql.functions import *
spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')

df=df.withColumn("year",year(col('order_date')))

df.display()

# COMMAND ----------

df.write.format("delta").mode("append").save("abfss://silver@databricksretailsa.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS databricksretail_cat.silver.orders 
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricksretailsa.dfs.core.windows.net/orders"