# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where Customer Id not null
# MAGIC

# COMMAND ----------

df = spark.sql("SELECT * ")

# COMMAND ----------

# MAGIC %md
# MAGIC <b> 
# MAGIC Remove records where gender is other than Male/Female
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Outlier check at some registration_date > DOb.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge the data into silver layer while adding current_timestamp
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the flag in the bronze layer
