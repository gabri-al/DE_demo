# Databricks notebook source
# MAGIC %md
# MAGIC ## Orders Silver Layer

# COMMAND ----------

# MAGIC %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if customer ID exists

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
def generate_flags(customerids: pd.Series) -> pd.Series:
  return customerids.apply(validate_customer)

# COMMAND ----------

df = (spark.table("orders_bronze")
        .filter('customerid is not null')
        .withColumn('customerid_is_valid', generate_flags('customerid'))
        .createOrReplaceTempView("orders_silver_1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from orders_silver_1 where customerid_is_valid = 'N';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC with N_ids as (
# MAGIC   select distinct customerid from orders_silver_1 where customerid_is_valid = 'N'
# MAGIC )
# MAGIC   select * from customers_silver 
# MAGIC   where clientid in (select * from N_ids);
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_silver a
# MAGIC USING (select distinct orderid, transactiondate, items, amount, customerid
# MAGIC       from orders_silver_1 where customerid_is_valid = 'Y') b
# MAGIC ON a.orderid = b.orderid
# MAGIC WHEN NOT MATCHED THEN
# MAGIC  INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*), count(distinct orderid) from orders_silver;
