# Databricks notebook source
df=spark.read.table("databricksretail_cat.bronze.regions")

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksretailsa.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS databricksretail_cat.silver.regions 
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricksretailsa.dfs.core.windows.net/regions"