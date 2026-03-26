# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE silverlayer.Customer(
# MAGIC     customer_id int,
# MAGIC     first_name string,
# MAGIC     last_name string,
# MAGIC     email string,
# MAGIC     phone string,
# MAGIC     country string,
# MAGIC     city string,
# MAGIC     registration_date timestamp,
# MAGIC     date_of_birth timestamp,
# MAGIC     gender string,
# MAGIC     merged_timestamp timestamp
# MAGIC )USING DELTA LOCATION "abfss://silverlayer@smartpolicysystemadls.dfs.core.windows.net/Customer"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.customer
