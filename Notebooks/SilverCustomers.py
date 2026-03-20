# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Load from Bronze

# COMMAND ----------

df=spark.read.format("parquet")\
    .option("header","true")\
    .load("abfss://bronze@databricksretailsa.dfs.core.windows.net/customers")

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Extract Domain from email id

# COMMAND ----------

df=df.withColumn("domain",split(split(col("email"), "@")[1],"\.")[0])

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Concatenate First name and Last name to build Full name

# COMMAND ----------

df= df.withColumn("FullName",concat(col("first_name"),lit(" "),col("last_name")))
df= df.drop("first_name","last_name")

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Segregate Old customers and new customers into dataframes

# COMMAND ----------

if (spark.catalog.tableExists("databricksretail_cat.silver.Customers") == False ):
    df_existing=spark.sql("select '0' as customer_id, current_timestamp() as create_date where 1=2" )
else:
    df_existing=spark.sql("select customer_id, create_date from databricksretail_cat.silver.Customers")

df_old=df.join(df_existing,df.customer_id==df_existing.customer_id,"inner").select(df.customer_id.alias('customer_id'),df_existing.create_date.alias('create_date'))

df_new=df.join(df_existing,df.customer_id==df_existing.customer_id,"leftanti").select(df.customer_id.alias('customer_id'))




# COMMAND ----------

# MAGIC %md
# MAGIC **SCT Type1**
# MAGIC Join DF with new customers ( Create and update date for these customers is current timestamp)

# COMMAND ----------

df1=df.join(df_new, 'customer_id', 'inner').select(df.customer_id,df.email,df.city,df.state,df.domain,df.FullName)\
    .withColumn("create_date",current_timestamp())\
    .withColumn("update_date",current_timestamp())

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Join DF with old customers ( All fields are picked up except create date, update date is current timestamp)

# COMMAND ----------

df2=df.join(df_old, 'customer_id', 'inner').select(df.customer_id,df.email,df.city,df.state,df.domain,df.FullName,df_old.create_date)\
     .withColumn("update_date",current_timestamp())

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write Union of both old and new customers to SILVER customers

# COMMAND ----------

df1.union(df2).write.mode("overwrite").format("delta").option("mergeSchema", "true").save("abfss://silver@databricksretailsa.dfs.core.windows.net/customers")


# COMMAND ----------

# MAGIC %md
# MAGIC Create the same as table under Silver schema

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS databricksretail_cat.silver.Customers 
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricksretailsa.dfs.core.windows.net/customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`abfss://silver@databricksretailsa.dfs.core.windows.net/customers`
# MAGIC
# MAGIC select * from  databricksretail_cat.silver.Customers 