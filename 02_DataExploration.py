# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# MAGIC %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data in Temp View

# COMMAND ----------

# Read the CSV file from the volume
df = (spark.
      read.
      format("csv").
      options(sep="|", header=True).
      load("/Volumes/"+_catalog+'/'+_schema+'/'+_volume+'/'+"customers_001.csv").
      createOrReplaceTempView("customers_tv"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_tv limit 6;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_tv limit 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) tot_recs, count(distinct clientid) tot_distinct from customers_tv;

# COMMAND ----------

# MAGIC %sql
# MAGIC with dupes as (
# MAGIC   select clientid, count(*) from customers_tv group by 1 having count(*) > 1
# MAGIC )
# MAGIC select *
# MAGIC from customers_tv
# MAGIC where clientid in (select clientid from dupes)
# MAGIC order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC with d0 as (
# MAGIC   select distinct * from customers_tv
# MAGIC )
# MAGIC   select count(*) recs_, count(distinct clientid) distinct_ from d0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist & Describe a Delta Table

# COMMAND ----------

# Persist data to a Delta table
spark.sql(f"""
  CREATE OR REPLACE TABLE customers_test
    (clientid STRING, firstname STRING, lastname STRING, email STRING, address STRING, country STRING, phone STRING,
    channel STRING, creation_date TIMESTAMP)
  COMMENT "Staging table containing customer data with PII information"
""")

spark.sql(f"""
  INSERT INTO customers_test 
    select * from customers_tv
""")

# COMMAND ----------

# Summarize table stats
df__ = spark.table(_catalog+'.'+_schema+'.'+"customers_test")
dbutils.data.summarize(df__)

# COMMAND ----------

# The table still contains duplicates! Remove them
spark.sql("TRUNCATE TABLE customers_test")

spark.sql(f"""
  INSERT INTO customers_test 
    select distinct * from customers_tv
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from customers_test

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from customers_test version as of 1

# COMMAND ----------


