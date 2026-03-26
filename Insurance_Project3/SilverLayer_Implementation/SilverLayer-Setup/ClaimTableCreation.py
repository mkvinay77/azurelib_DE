# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silverlayer.Claim(
# MAGIC   claim_id int,
# MAGIC   policy_id int,
# MAGIC   date_of_claim DATE,
# MAGIC   claim_amount double,
# MAGIC   claim_status string,
# MAGIC   LastUpdatedTimeStamp timestamp,
# MAGIC   merege_timestamp timestamp
# MAGIC ) USING DELTA LOCATION "abfss://silverlayer@smartpolicysystemadls.dfs.core.windows.net/Claim"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.Claim
