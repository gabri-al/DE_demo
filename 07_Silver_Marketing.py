# Databricks notebook source
# MAGIC %md
# MAGIC ## Marketing Silver Layer

# COMMAND ----------

# %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if customer ID exists using Pandas UDFs

# COMMAND ----------

_catalog = dbutils.widgets.get("_catalog")
_schema = dbutils.widgets.get("_schema")
_volume = dbutils.widgets.get("_volume")

spark.sql("CREATE CATALOG IF NOT EXISTS "+_catalog)
spark.sql("CREATE SCHEMA IF NOT EXISTS "+_catalog+"."+_schema)
spark.sql("CREATE VOLUME IF NOT EXISTS "+_catalog+"."+_schema+"."+_volume)
spark.sql("USE CATALOG "+_catalog)
spark.sql("USE SCHEMA "+_schema)

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# Create a set of valid customer IDs
valid_clients = set(list(spark.sql("select distinct clientid from customers_silver").toPandas()["clientid"]))

# COMMAND ----------

# Define customer validation function
def validate_customer(id_):
    return 'Y' if id_ in valid_clients else 'N'

# Wrap function into a Pandas UDF
@F.pandas_udf("string")
def generate_flags(customerid: pd.Series) -> pd.Series:
  return customerid.apply(validate_customer)

# COMMAND ----------

df = (spark.table("marketing_bronze")
        .filter('targetedcustomerid is not null')
        .withColumn('customerid_is_valid', generate_flags('targetedcustomerid'))
        .createOrReplaceTempView("marketing_silver_1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from marketing_silver_1 where customerid_is_valid = 'N';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*with N_ids as (
# MAGIC   select distinct targetedcustomerid from marketing_silver_1 where customerid_is_valid = 'N'
# MAGIC )
# MAGIC   select * from customers_silver 
# MAGIC   where clientid in (select * from N_ids);
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO marketing_silver a
# MAGIC USING (select distinct marketingid, marketingdate, discount, clicked, targetedcustomerid
# MAGIC       from marketing_silver_1 where customerid_is_valid = 'Y') b
# MAGIC ON a.marketingid = b.marketingid
# MAGIC WHEN NOT MATCHED THEN
# MAGIC  INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*), count(distinct marketingid) from marketing_silver;
