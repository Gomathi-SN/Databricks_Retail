# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Data Ingestion

# COMMAND ----------

pFilename=dbutils.widgets.get("pFilename")

# COMMAND ----------

df=spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation",f"abfss://bronze@databricksretailsa.dfs.core.windows.net/checkpoint_{pFilename}")\
    .load(f"abfss://sourcedata@databricksretailsa.dfs.core.windows.net/{pFilename}/")

# COMMAND ----------


df.writeStream.format("parquet") \
        .outputMode("append")\
        .option("checkpointLocation",f"abfss://bronze@databricksretailsa.dfs.core.windows.net/checkpoint_{pFilename}")\
        .option("path",f"abfss://bronze@databricksretailsa.dfs.core.windows.net/{pFilename}")\
        .trigger(once=True)\
        .start()


# COMMAND ----------


