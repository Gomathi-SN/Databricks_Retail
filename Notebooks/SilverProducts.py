# Databricks notebook source
df=spark.read.format("parquet")\
    .option("header", "true")\
    .load("abfss://bronze@databricksretailsa.dfs.core.windows.net/products")
df.count()

# COMMAND ----------

from pyspark.sql.functions import * 

df.dropDuplicates(["product_id"])

# COMMAND ----------

df.createOrReplaceTempView("product")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE oR REPLACE FUNCTION databricksretail_cat.bronze.APPLY_DISCOUNT(price double)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN price * 0.95
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *, databricksretail_cat.bronze.APPLY_DISCOUNT(price) as discountedprice  from product

# COMMAND ----------

df=df.withColumn("discountedprice",expr("databricksretail_cat.bronze.apply_discount(price)"))

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION databricksretail_cat.bronze.case_func(product string)
# MAGIC returns STRING
# MAGIC LANGUAGE Python
# MAGIC as 
# MAGIC $$
# MAGIC   return product.upper()
# MAGIC $$

# COMMAND ----------

df=df.withColumn("product_name",expr("databricksretail_cat.bronze.case_func(product_name)"))

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksretailsa.dfs.core.windows.net/products")