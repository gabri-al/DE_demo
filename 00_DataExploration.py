# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC From the **Workspace** section you can:
# MAGIC - Configure compute
# MAGIC - Access UC
# MAGIC - Set up a Git Folder
# MAGIC - Navigate in your workspace's folders and notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC Within a **Notebook**:
# MAGIC - We have a main coding language
# MAGIC - We can use magic commands to switch language and run different operations
# MAGIC
# MAGIC **What are Magic commands?**
# MAGIC - `%python %r %sql %scala` switch cell programming language
# MAGIC - `%sh` run shell code on the driver
# MAGIC - `%md` style a cell as markdown
# MAGIC - `%pip` install new Python libraries
# MAGIC - `%run` run a remote Notebook
# MAGIC - `%fs` shortcut for `dbutils` filesystem commands

# COMMAND ----------

# DBTITLE 1,Run a remote Notebook
# MAGIC %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data as Temp View

# COMMAND ----------

# DBTITLE 1,Create a temp view from a spark df
# Read the CSV file from Volume
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
# MAGIC ## Validate data

# COMMAND ----------

# DBTITLE 1,Check for duplicates using SQL
# MAGIC %sql
# MAGIC select count(*) tot_recs, count(distinct clientid) tot_distinct from customers_tv;

# COMMAND ----------

# DBTITLE 1,Run SQL CTE
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

# DBTITLE 1,By default, CREATE TABLE writes to Delta
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

# DBTITLE 1,Summarize
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
# MAGIC ## Delta Format Overview

# COMMAND ----------

# DBTITLE 1,Describe Delta
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
# MAGIC ### Time Travel

# COMMAND ----------

# DBTITLE 1,Access Available History
# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from customers_test;

# COMMAND ----------

# DBTITLE 1,Query History by Version
# MAGIC %sql
# MAGIC Select count(*) from customers_test version as of 9;

# COMMAND ----------

# DBTITLE 1,Query History by Timestamp
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

# DBTITLE 1,Rollback
# MAGIC %sql
# MAGIC -- Restore to a previous version
# MAGIC RESTORE TABLE customers_test TO VERSION AS OF 10;
# MAGIC select count(*) from customers_test;

# COMMAND ----------

# DBTITLE 1,VACUUM
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
# MAGIC ALTER TABLE customers_test SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '0 hour'); -- Modify accordignly
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
# MAGIC -- What's happening here: File read error
# MAGIC select * from customers_test VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore configuration
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column and row masking

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_test limit 6;

# COMMAND ----------

# DBTITLE 1,Column Masking
# MAGIC %sql
# MAGIC -- Create a function to encrypt a column
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING)
# MAGIC  RETURNS STRING
# MAGIC  RETURN IF(
# MAGIC   IS_MEMBER('field-eng-only'), -- Mask only for a group in this example
# MAGIC   CONCAT( LEFT(email, 1), REPEAT("*", LENGTH(email) - 1)),
# MAGIC   email);
# MAGIC
# MAGIC ALTER TABLE customers_test ALTER COLUMN email SET MASK mask_email;
# MAGIC
# MAGIC select * from customers_test limit 6;

# COMMAND ----------

# MAGIC %md
# MAGIC **How do we undo the masking?**
# MAGIC *--> Ask the assistant!*
# MAGIC
# MAGIC How to undo this operation: ALTER TABLE customers_test ALTER COLUMN email SET MASK mask_email ??

# COMMAND ----------

# Undo Masking

### -- > FILL IN

# COMMAND ----------

# MAGIC %sql
# MAGIC select channel, count(*) customers
# MAGIC from customers_test
# MAGIC group by 1 order by 1;

# COMMAND ----------

# DBTITLE 1,Row Filtering
# MAGIC %sql
# MAGIC -- Create a function to exclude rows
# MAGIC CREATE OR REPLACE FUNCTION filter_channel(c STRING)
# MAGIC  RETURN IF(
# MAGIC   IS_MEMBER('ultra-admin'), -- Mask only for a group in this example
# MAGIC   true,
# MAGIC   c not in ('Call Center') -- When the is_member() condition fails, the table is filtered on this condition
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE customers_test SET ROW FILTER filter_channel ON (channel);

# COMMAND ----------

# MAGIC %sql
# MAGIC select channel, count(*) customers
# MAGIC from customers_test
# MAGIC group by 1 order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove row filtering
# MAGIC ALTER TABLE customers_test DROP ROW FILTER;
