# Databricks notebook source
# MAGIC %md
# MAGIC # Write Bronze Layer

# COMMAND ----------

# %run ../DE_demo/00_GlobalVars

# COMMAND ----------

file_seq = '00' + str(dbutils.widgets.get("task_i"))
_catalog = dbutils.widgets.get("_catalog")
_schema = dbutils.widgets.get("_schema")
_volume = dbutils.widgets.get("_volume")

spark.sql("CREATE CATALOG IF NOT EXISTS "+_catalog)
spark.sql("CREATE SCHEMA IF NOT EXISTS "+_catalog+"."+_schema)
spark.sql("CREATE VOLUME IF NOT EXISTS "+_catalog+"."+_schema+"."+_volume)
spark.sql("USE CATALOG "+_catalog)
spark.sql("USE SCHEMA "+_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# Read the CSV file from the volume
df = (spark.
      read.
      format("csv").
      options(sep="|", header=True).
      load("/Volumes/"+_catalog+'/'+_schema+'/'+_volume+'/'+"customers_"+file_seq+".csv").
      createOrReplaceTempView("customers_file"))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers_bronze
# MAGIC select * from customers_file;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders

# COMMAND ----------

# Read the CSV file from the volume
df = (spark.
      read.
      format("csv").
      options(sep="|", header=True).
      load("/Volumes/"+_catalog+'/'+_schema+'/'+_volume+'/'+"orders_"+file_seq+".csv").
      createOrReplaceTempView("orders_file"))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_bronze
# MAGIC select * from orders_file;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Marketing

# COMMAND ----------

# Read the CSV file from the volume
df = (spark.
      read.
      format("csv").
      options(sep="|", header=True).
      load("/Volumes/"+_catalog+'/'+_schema+'/'+_volume+'/'+"marketing_"+file_seq+".csv").
      createOrReplaceTempView("marketing_file"))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO marketing_bronze
# MAGIC select * from marketing_file;
