# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation

# COMMAND ----------

# MAGIC %md
# MAGIC This data generation is inspired by [this demo](https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/orchestrate-and-run-your-dbt-jobs?itm_data=demo_center) from db demos.

# COMMAND ----------

# Import libraries
from faker import Faker
import pandas as pd
from collections import OrderedDict 
from datetime import datetime, timedelta

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Global Variables

# COMMAND ----------

# Initialize Faker
fake = Faker()

# Dataset Size
Ncustomers = 100000
Norders = 3 * Ncustomers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customers

# COMMAND ----------

# Generate customers
def gen_customers(N):

  # Variables
  channels_ = OrderedDict([("Web", 0.3),("Call Center", 0.2),("App", 0.5)])
  countries_ = OrderedDict([("Italy", 0.05),("USA", 0.4),("Brasil", 0.4),("Portugal", 0.15)])

  # Initialize empty lists
  id_ = []; first_name = []; last_name = []; email = []; address = []; country = []; phone = []; channel = []
  
  # Generate data
  for i in range(N):

    # Define random seed
    fake.seed_instance(1000+i)

    # Generate data
    id_.append(fake.uuid4())
    first_name.append(fake.first_name())
    last_name.append(fake.last_name())
    email.append(fake.ascii_company_email())
    address.append(fake.address())
    country.append(fake.random_elements(elements = countries_, length = 1)[0])
    phone.append(fake.phone_number())
    channel.append(fake.random_elements(elements = channels_, length = 1)[0])

  return pd.DataFrame({
    'id': id_, 'firstname': first_name, 'lastname': last_name,
    'email': email, 'address': address, 'country': country,
    'phone': phone, 'channel': channel})

# COMMAND ----------

df_customers = gen_customers(Ncustomers)
display(df_customers.head(7))

# COMMAND ----------


