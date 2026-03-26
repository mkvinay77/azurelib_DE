# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silverlayer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silverlayer.Agent(
# MAGIC   agent_id integer,
# MAGIC   agent_name string,
# MAGIC   agent_email string,
# MAGIC   agent_phone string,
# MAGIC   branch_id integer,
# MAGIC   create_timestamp timestamp,
# MAGIC   merged_timestamp timestamp
# MAGIC ) USING DELTA LOCATION "abfss://silverlayer@smartpolicysystemadls.dfs.core.windows.net/Agent"

# COMMAND ----------

# #missed to add the merge_timestamp so
# %sql
# CREATE OR REPLACE TABLE silverlayer.Agent (
# agent_id integer,
# agent_name string,
# agent_email string,
# agent_phone string,
# branch_id integer,
# create_timestamp timestamp,
# merged_timestamp timestamp
# ) USING DELTA LOCATION "abfss://silverlayer@smartpolicysystemadls.dfs.core.windows.net/Agent"
