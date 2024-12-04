# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Order Attribution to Marketing Activities

# COMMAND ----------

# MAGIC %run ../DE_demo/00_GlobalVars

# COMMAND ----------

dbutils.widgets.dropdown("attribution_window", "7", ["7", "10"], "Choose Attribution")
attr_window = int(dbutils.widgets.get("attribution_window"))
print(attr_window)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Orders with Marketing

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from marketing_silver limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW attribution_staging_01 AS
# MAGIC with join_data as (
# MAGIC   select a.*, b.*
# MAGIC   from orders_silver a
# MAGIC   left join marketing_silver b
# MAGIC     on a.customerid = b.targetedcustomerid
# MAGIC     and b.clicked = 1
# MAGIC     and a.transactiondate between b.marketingdate and b.marketingdate + interval '${attribution_window}' day
# MAGIC )
# MAGIC   select *,
# MAGIC   case when 
# MAGIC     LAG (orderid, 1) OVER (partition by orderid order by discount desc, marketingdate desc) = orderid 
# MAGIC     then 1 else 0 end as double_attribution
# MAGIC   from join_data
# MAGIC   order by orderid, discount desc, marketingdate desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for double attributions
# MAGIC with d0 as (
# MAGIC   select orderid, count(distinct marketingid) m_rec
# MAGIC   from attribution_staging_01
# MAGIC   where marketingid is not null
# MAGIC   group by 1 having m_rec > 1
# MAGIC )
# MAGIC   select *
# MAGIC   from attribution_staging_01
# MAGIC   where orderid in (select distinct orderid from d0);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join with Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW attribution_staging_02 AS
# MAGIC   select a.*, b.*
# MAGIC   from attribution_staging_01 as a
# MAGIC   left join customers_silver as b
# MAGIC   on a.customerid = b.clientid; -- and date(b.creation_date) <= date(a.transactiondate)
# MAGIC   --where a.double_attribution = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from attribution_staging_02 where marketingid is not null and clientid is not null;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write valid records
# MAGIC drop table if exists gold_order_attribution;
# MAGIC create table gold_order_attribution as
# MAGIC   select * EXCEPT (double_attribution)
# MAGIC   from attribution_staging_02
# MAGIC   where double_attribution = 0 
# MAGIC     and clientid is not none
# MAGIC     and date(creation_date) <= date(transactiondate); -- client existing before their first order
