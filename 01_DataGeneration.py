# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation

# COMMAND ----------

# MAGIC %md
# MAGIC This data generation is inspired by [this demo](https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/orchestrate-and-run-your-dbt-jobs?itm_data=demo_center) from db demos.

# COMMAND ----------

# %run ../DE_demo/00_GlobalVars

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
from faker import Faker
import pandas as pd
from collections import OrderedDict 
from datetime import datetime, timedelta
import random

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Variables

# COMMAND ----------

i_ = int(dbutils.widgets.get("task_i"))
_catalog = dbutils.widgets.get("_catalog")
_schema = dbutils.widgets.get("_schema")
_volume = dbutils.widgets.get("_volume")
SEEDS_ = list(dbutils.widgets.get("SEEDS_"))

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS "+_catalog)
spark.sql("CREATE SCHEMA IF NOT EXISTS "+_catalog+"."+_schema)
spark.sql("CREATE VOLUME IF NOT EXISTS "+_catalog+"."+_schema+"."+_volume)
spark.sql("USE CATALOG "+_catalog)
spark.sql("USE SCHEMA "+_schema)

# COMMAND ----------

# Initialize Faker
fake = Faker()
SEED_START = int(SEEDS_[i_])

# Dataset Size
Ncustomers = 25000
Norders = round(1.5 * Ncustomers)
Nmarketing = round(2.5 * Ncustomers)

# Dates
start = datetime.now() - timedelta(days=30*30)
end = datetime.now() - timedelta(days=4*30)

# Path to volume to write csv
path_volume_ = f"/Volumes/{_catalog}/{_schema}/{_volume}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customers Data

# COMMAND ----------

# Generate customers function
def gen_customers(N, with_duplicates = True):

  # Variables
  channels_ = OrderedDict([("Web", 0.3),("Call Center", 0.2),("App", 0.5)])
  countries_ = OrderedDict([("Italy", 0.05),("USA", 0.4),("Brasil", 0.4),("Portugal", 0.15)])

  # Initialize empty lists
  id_ = []; first_name = []; last_name = []; email = []; address = []; country = []; phone = []; channel = []; dates = []
  
  # Generate data
  for i in range(N):

    # Define random seed
    fake.seed_instance(SEED_START+i)

    # Generate data
    id_.append('c-'+str(fake.uuid4()))
    first_name.append(fake.first_name().replace(',',''))
    last_name.append(fake.last_name().replace(',',''))
    email.append(fake.ascii_company_email())
    address.append(fake.address().replace(',','').replace('\n',' '))
    country.append(fake.random_elements(elements = countries_, length = 1)[0])
    phone.append(fake.phone_number().replace(',',''))
    channel.append(fake.random_elements(elements = channels_, length = 1)[0])
    dates.append(fake.date_time_between(start_date=start, end_date=end).strftime("%Y-%m-%d %H:%M:%S"))

  # Add duplicates
  if with_duplicates:
    tot_ = round(N * .04)
    duplicate_ids = random.choices(range(N), k = tot_)
    for i in duplicate_ids:
      id_.append(id_[i])
      first_name.append(first_name[i])
      last_name.append(last_name[i])
      email.append(email[i])
      address.append(address[i])
      country.append(country[i])
      phone.append(phone[i])
      channel.append(channel[i])
      dates.append(dates[i])

  return pd.DataFrame({
    'clientid': id_, 'firstname': first_name, 'lastname': last_name,
    'email': email, 'address': address, 'country': country,
    'phone': phone, 'channel': channel, 'creation_date': dates})

# COMMAND ----------

df_customers = gen_customers(Ncustomers)
df_customers.sort_values('clientid', inplace=True)
display(df_customers.head(7))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Orders Data

# COMMAND ----------

def gen_orders(N, with_duplicates = True):

  # Initialize empty lists
  id_ = []; dates = []; items = []; amount = []
  
  # Generate data
  customerid = random.choices(list(df_customers['clientid']), k=N)
  for i in range(N):

    # Define random seed
    fake.seed_instance(2*SEED_START + i)

    # Generate data
    id_.append('o-'+fake.uuid4())
    dates.append(fake.date_time_between(start_date=start, end_date=end).strftime("%Y-%m-%d %H:%M:%S"))
    item = round(fake.random.uniform(1, 4))
    items.append(item)
    amount.append(round( item * (fake.random.uniform(0,1) + 4)  ) )

  # Add duplicates
  if with_duplicates:
    tot_ = round(N * .048)
    duplicate_ids = random.choices(range(N), k = tot_)
    for i in duplicate_ids:
      id_.append(id_[i])
      dates.append(dates[i])
      items.append(items[i])
      amount.append(amount[i])
      customerid.append(customerid[i])

  return pd.DataFrame({
    'orderid': id_, 'transactiondate': dates,
    'items': items, 'amount': amount, 'customerid': customerid})

# COMMAND ----------

df_orders = gen_orders(Norders)
df_orders.sort_values('orderid', inplace=True)
display(df_orders.head(7))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Marketing Activity

# COMMAND ----------

# Generate marketing data function
def gen_marketing(N):

  # Variables
  marketing_activities_ = OrderedDict([("SMS", 0.33),("email", 0.33),("bunner", 0.33)])

  # Initialize empty lists
  id_ = []; dates = []; activity = []; clicked = []; discount = []
  
  # Generate data
  customerid = random.choices(list(df_customers['clientid']), k=N)
  for i in range(N):

    # Define random seed
    fake.seed_instance(3*SEED_START + i)

    # Generate data
    id_.append('a-'+fake.uuid4())
    dates.append(fake.date_time_between(start_date=start, end_date=end).strftime("%Y-%m-%d %H:%M:%S"))
    clicked.append(round(fake.random.uniform(0, 1)))
    discount.append(round(fake.random.uniform(0, 0.41), 2))

  return pd.DataFrame({
    'marketingid': id_, 'marketingdate': dates, 'discount': discount,
    'clicked': clicked, 'targetedcustomerid': customerid})

# COMMAND ----------

df_marketing = gen_marketing(Nmarketing)
display(df_marketing.head(7))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to CSV

# COMMAND ----------

# Customers
df_customers1 = df_customers.iloc[:-22, :]
df_customers1.to_csv(path_volume_+"/customers_00"+str(i_)+".csv", index=False, mode='w', sep = '|')

# Orders
df_orders.to_csv(path_volume_+"/orders_00"+str(i_)+".csv", index=False, mode='w', sep = '|')

# Marketing Activity
df_marketing.to_csv(path_volume_+"/marketing_00"+str(i_)+".csv", index=False, mode='w', sep = '|')
