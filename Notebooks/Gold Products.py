# Databricks notebook source
df=spark.sql("select product_id as dim_productID, product_name, category, brand, discountedprice from databricksretail_cat.silver.products")

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("databricksretail_cat.gold.dim_products")