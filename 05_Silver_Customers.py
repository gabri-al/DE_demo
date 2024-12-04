# Databricks notebook source
# MAGIC %md
# MAGIC ## Customers Silver Layer

# COMMAND ----------

# MAGIC %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Duplicates and Double emails

# COMMAND ----------

df = spark.sql(
"""
with d0 as (
  select distinct * from customers_bronze
), dup_emails as (
  select email, count(distinct clientid) recs
  from d0
  group by 1 having recs > 1
)
  select * from d0 where email not in (select email from dup_emails)
"""
).createOrReplaceTempView("customers_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customers_silver a
# MAGIC USING customers_clean b
# MAGIC ON a.clientid = b.clientid
# MAGIC WHEN NOT MATCHED THEN
# MAGIC  INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*), count(distinct clientid) from customers_silver;