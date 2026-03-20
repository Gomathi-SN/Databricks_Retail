# Databricks notebook source
from pyspark.sql.functions import *

spark.sql("""CREATE OR REPLACE VIEW databricksretail_cat.gold.vw_years
             as select distinct O.year 
              from databricksretail_cat.silver.orders O """)

# COMMAND ----------



spark.sql("""CREATE OR REPLACE VIEW databricksretail_cat.gold.vw_topCustomers 
             as select C.dim_cust_id ,C.FullName, year, sum(total_amount) as total_expenditure,
              RANK() OVER (partition by O.year order by  sum(total_amount) desc ) as CustomerRank
              from databricksretail_cat.silver.orders O
             join databricksretail_cat.gold.dim_customers C on O.customer_id=C.dim_cust_id
             group by  C.dim_cust_id ,C.FullName, year
                    """)




# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW databricksretail_cat.gold.vw_topProducts as
# MAGIC  select P.dim_productID ,P.product_name, year, sum(total_amount) as total_expenditure,
# MAGIC               RANK() OVER (partition by O.year order by  sum(total_amount) desc ) as CustomerRank
# MAGIC               from databricksretail_cat.silver.orders O
# MAGIC              join databricksretail_cat.gold.dim_products P on O.product_id=P.dim_productID
# MAGIC              group by  P.dim_productID ,P.product_name, year
# MAGIC                  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or Replace view databricksretail_cat.gold.vw_totalrevenue as
# MAGIC select year, sum(total_amount) as total_revenue from databricksretail_cat.gold.fact_orders
# MAGIC group by year