# Databricks notebook source
df=spark.sql("select customer_id as dim_cust_id, email,city, state, domain,FullName,create_date,update_date from databricksretail_cat.silver.customers")

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("databricksretail_cat.gold.dim_customers")
