# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Variables / Functions shared across the Repo

# COMMAND ----------

_catalog = 'users'
_schema = 'gabriele_albini'
_volume = 'DE_demo_land'

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS "+_catalog)
spark.sql("CREATE SCHEMA IF NOT EXISTS "+_catalog+"."+_schema)
spark.sql("CREATE VOLUME IF NOT EXISTS "+_catalog+"."+_schema+"."+_volume)
spark.sql("USE CATALOG "+_catalog)
spark.sql("USE SCHEMA "+_schema)

# COMMAND ----------

recreate_tbls = True

# COMMAND ----------

SEEDS_ = [301, 501, 801]
