-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Initialize Tables

-- COMMAND ----------

-- %run ../DE_demo/00_GlobalVars

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _catalog = dbutils.widgets.get("_catalog")
-- MAGIC _schema = dbutils.widgets.get("_schema")
-- MAGIC _volume = dbutils.widgets.get("_volume")
-- MAGIC recreate_tbls = bool(dbutils.widgets.get("recreate_tbls"))
-- MAGIC
-- MAGIC spark.sql("CREATE CATALOG IF NOT EXISTS "+_catalog)
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS "+_catalog+"."+_schema)
-- MAGIC spark.sql("CREATE VOLUME IF NOT EXISTS "+_catalog+"."+_schema+"."+_volume)
-- MAGIC spark.sql("USE CATALOG "+_catalog)
-- MAGIC spark.sql("USE SCHEMA "+_schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if recreate_tbls:
-- MAGIC   spark.sql("DROP TABLE customers_bronze")
-- MAGIC   spark.sql("DROP TABLE orders_bronze")
-- MAGIC   spark.sql("DROP TABLE marketing_bronze")
-- MAGIC   spark.sql("DROP TABLE customers_silver")
-- MAGIC   spark.sql("DROP TABLE orders_silver")
-- MAGIC   spark.sql("DROP TABLE marketing_silver")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create empty bronze tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers_bronze (
    clientid STRING,
    firstname STRING,
    lastname STRING,
    email STRING,
    address STRING,
    country STRING,
    phone STRING,
    channel STRING,
    creation_date TIMESTAMP
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS orders_bronze (
    orderid STRING,
    transactiondate TIMESTAMP,
    items INT,
    amount INT,
    customerid STRING
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS marketing_bronze (
    marketingid STRING,
    marketingdate TIMESTAMP,
    discount FLOAT,
    clicked INT,
    targetedcustomerid STRING
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create empty silver tables, with constraints

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers_silver (
    clientid STRING NOT NULL,
    firstname STRING,
    lastname STRING,
    email STRING,
    address STRING,
    country STRING,
    phone STRING,
    channel STRING,
    creation_date TIMESTAMP
);

ALTER TABLE customers_silver ADD CONSTRAINT clients_silver_pk PRIMARY KEY (clientid);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS orders_silver (
    orderid STRING NOT NULL,
    transactiondate TIMESTAMP,
    items INT,
    amount INT,
    customerid STRING
);

ALTER TABLE orders_silver ADD CONSTRAINT orders_silver_pk PRIMARY KEY (orderid);
ALTER TABLE orders_silver ADD CONSTRAINT orders_silver_fk FOREIGN KEY (customerid) REFERENCES customers_silver (clientid);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS marketing_silver (
    marketingid STRING NOT NULL,
    marketingdate TIMESTAMP,
    discount FLOAT,
    clicked INT,
    targetedcustomerid STRING
);

ALTER TABLE marketing_silver ADD CONSTRAINT marketing_silver_pk PRIMARY KEY (marketingid);
ALTER TABLE marketing_silver ADD CONSTRAINT marketing_silver_fk FOREIGN KEY (targetedcustomerid) REFERENCES customers_silver (clientid);
