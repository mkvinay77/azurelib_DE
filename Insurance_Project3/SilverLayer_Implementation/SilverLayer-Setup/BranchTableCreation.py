# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE silverlayer.Branch(
# MAGIC   branch_id INT,
# MAGIC   branch_country STRING,
# MAGIC   branch_city STRING,
# MAGIC   merged_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION "abfss://silverlayer@smartpolicysystemadls.dfs.core.windows.net/Branch";
