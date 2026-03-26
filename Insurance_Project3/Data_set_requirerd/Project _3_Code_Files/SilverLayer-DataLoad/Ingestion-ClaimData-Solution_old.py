# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where claim_id, policy_id is ,claim status,claim_amount,lastupdated null
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where policy_id Id not exist in Policy table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Convert date_of_claim to Date column with formate (mm-dd-yyyy)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(date_of_claim, 'yyyy-MM-dd') FROM bronzelayer.claim  where LastUpdatedTimeStamp is null

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure  claim amount is >0

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Add the merged_date_timestamp (current timesatmp)
