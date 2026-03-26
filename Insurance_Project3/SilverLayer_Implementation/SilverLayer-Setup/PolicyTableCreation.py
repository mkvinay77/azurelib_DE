# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silverlayer.Policy(
# MAGIC   policy_id int, 
# MAGIC   policy_type string,
# MAGIC   customer_id int,
# MAGIC   start_date timestamp,
# MAGIC   end_date timestamp,
# MAGIC   premium double,
# MAGIC   coverage_amount double,
# MAGIC   merge_timestamp TIMESTAMP
# MAGIC )USING DELTA LOCATION "abfss://silverlayer@smartpolicysystemadls.dfs.core.windows.net/Policy"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.policy
