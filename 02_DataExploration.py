# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# MAGIC %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data as Temp View

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
# MAGIC select count(*) tot_recs, count(distinct clientid) tot_distinct from customers_tv;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore duplicated records
# MAGIC with dupes as (
# MAGIC   select clientid, count(*) from customers_tv group by 1 having count(*) > 1
# MAGIC )
# MAGIC select *
# MAGIC from customers_tv
# MAGIC where clientid in (select clientid from dupes)
# MAGIC order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean duplicates and revalidate
# MAGIC with d0 as (
# MAGIC   select distinct * from customers_tv
# MAGIC )
# MAGIC   select count(*) recs_, count(distinct clientid) distinct_ from d0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check emails
# MAGIC with d0 as (
# MAGIC   select distinct * from customers_tv
# MAGIC ), d1 as (
# MAGIC select email, count(*) recs
# MAGIC from d0
# MAGIC group by 1 having recs > 1
# MAGIC )
# MAGIC select *
# MAGIC from d0
# MAGIC where email in (select email from d1)
# MAGIC order by email asc, creation_date desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist to a Delta Table

# COMMAND ----------

# Persist data to a Delta table

# Table Definition
spark.sql(f"""
  CREATE OR REPLACE TABLE customers_test
    (clientid STRING, firstname STRING, lastname STRING, email STRING, address STRING, country STRING, phone STRING,
    channel STRING, creation_date TIMESTAMP)
  COMMENT "Staging table containing customer data with PII information"
""")

# Insert Records
spark.sql(f"""
  INSERT INTO customers_test 
    select distinct * from customers_tv
""")

# COMMAND ----------

# Summarize table stats
df__ = spark.table(_catalog+'.'+_schema+'.'+"customers_test")
dbutils.data.summarize(df__)

# COMMAND ----------

# The table still contains duplicate emails! Remove them
spark.sql(f"""
  with emails_ as (
    select email, count(*) recs
    from customers_test
    group by 1 having recs > 1
  )
  delete from customers_test where email in (select email from emails_)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Describe Delta Table

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

# MAGIC %md
# MAGIC ## Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from customers_test version as of 9;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_test TIMESTAMP AS OF '2024-12-04T07:33:21.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete a new record
# MAGIC delete from customers_test where clientid = 'c-03c7ba57-d519-49e7-80a5-d5b98a219605';
# MAGIC
# MAGIC -- Check history
# MAGIC DESCRIBE HISTORY customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Last version count
# MAGIC select count(*) from customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore to a previous version
# MAGIC RESTORE TABLE customers_test TO VERSION AS OF 10;
# MAGIC select count(*) from customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up history
# MAGIC VACUUM customers_test RETAIN 0 HOURS DRY RUN;

# COMMAND ----------

## Time travel is allowed by default for 30 days
## Vacuum will clean stale old files, keeping the last 7 days ones
print(spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")) # Run on classic compute

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC ALTER TABLE customers_test SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '1 hour');
# MAGIC VACUUM customers_test DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Still see the full history (Databricks automatically cleans up log entries older than this retention interval)
# MAGIC DESCRIBE HISTORY customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What's happening here
# MAGIC select * from customers_test VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore configuration
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = true;
