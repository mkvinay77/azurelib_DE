# Databricks notebook source
# MAGIC %md
# MAGIC **Sales By Policy Type and Month:** 
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance different policy types over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE goldlayer.sales_by_policy_type_and_month(
# MAGIC   policy_type STRING,
# MAGIC   sale_month STRING,
# MAGIC   total_premium INTEGER,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC   ) USING DELTA LOCATION "abfss://goldlayer@smartpolicysystemadls.dfs.core.windows.net/sales_by_policy_type_and_month"

# COMMAND ----------

# MAGIC %md
# MAGIC **Claims By Policy Type and Status:**
# MAGIC This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claim process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE goldlayer.claims_by_policy_type_and_status(
# MAGIC   policy_type STRING,
# MAGIC   claim_status STRING,
# MAGIC   total_claims INTEGER,
# MAGIC   total_claim_amount INTEGER,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC   ) USING DELTA LOCATION "abfss://goldlayer@smartpolicysystemadls.dfs.core.windows.net/claims_by_pilicy_type_and_status"

# COMMAND ----------

# MAGIC %md
# MAGIC **Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE goldlayer.claims_analysis(
# MAGIC   policy_type STRING,
# MAGIC   claim_status STRING,
# MAGIC   avg_claim_amount INTEGER,
# MAGIC   max_claim_amount INTEGER,
# MAGIC   min_claim_amount INTEGER,
# MAGIC   total_claims INTEGER,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC   ) USING DELTA LOCATION "abfss://goldlayer@smartpolicysystemadls.dfs.core.windows.net/claims_analysis"
