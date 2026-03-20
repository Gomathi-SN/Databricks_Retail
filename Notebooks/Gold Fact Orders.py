# Databricks notebook source
df=spark.sql("""select order_id,C.dim_cust_id ,P.dim_productID ,order_date,quantity,total_amount, year from databricksretail_cat.silver.orders O
             join databricksretail_cat.gold.dim_customers C on O.customer_id=C.dim_cust_id
             join databricksretail_cat.gold.dim_products P on O.product_id=P.dim_productID """)

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("databricksretail_cat.gold.Fact_orders")