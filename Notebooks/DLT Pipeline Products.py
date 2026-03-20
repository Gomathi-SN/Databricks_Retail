# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC SCD TYpe 2 using DLT 

# COMMAND ----------

@dlt.table

def products_ScdType2():
    df=spark.readStream.load("abfss://silver@databricksretailsa.dfs.core.windows.net/products")
    return df



# COMMAND ----------



# COMMAND ----------

@dlt.view

def products_vw():
    df=spark.readStream.table("LIVE.products_ScdType2")
    return df

# COMMAND ----------

dlt.create_streaming_table("products")

dlt.apply_changes(
    source='LIVE.products_ScdType2',
    target='products',
    keys=['product_id'],
    sequence_by='product_id',
    stored_as_scd_type=2
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Expectations

# COMMAND ----------

rules={
  "rule1":"product_id is not null",
  "rule2":"product_name is not null"
}

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(rules)

def Products_Stage():
    df=spark.readStream.table("databricksretail_cat.silver.products")
    return df
    

