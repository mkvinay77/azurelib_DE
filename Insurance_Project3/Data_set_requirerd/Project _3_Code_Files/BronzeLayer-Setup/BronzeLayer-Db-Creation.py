# Databricks notebook source
# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC create DATABASE bronzelayer;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronzelayer;
# MAGIC show tables;

# COMMAND ----------

spark.sql("drop DATABASE bronzelayer")

# COMMAND ----------

spark.sql("create DATABASE bronzelayer")
