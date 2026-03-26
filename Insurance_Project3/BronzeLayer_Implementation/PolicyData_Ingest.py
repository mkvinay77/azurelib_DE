# Databricks notebook source
# MAGIC %md
# MAGIC #PolicyData ingestion, new column, Delta table creation, move the data from the landing to processed

# COMMAND ----------

from pyspark.sql.functions import *
policy_schema = "policy_id int, policy_type string,customer_id int,start_date timestamp,end_date timestamp,premium double,coverage_amount double"
df = spark.read.json("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/PolicyData/*.json",schema=policy_schema)
display(df)

# COMMAND ----------

#adding merge_flag column
from pyspark.sql.functions import lit
df_merge_falg = df.withColumn("merge_falg", lit(False))
display(df_merge_falg)

# COMMAND ----------

#Creating the table in bronzelayer
df_merge_falg.write.mode("append").option("path","abfss://bronzelayer@smartpolicysystemadls.dfs.core.windows.net/PolicyData/").saveAsTable("bronzelayer.Policy")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronzelayer.Policy

# COMMAND ----------

#Now moving the data from landing to processed container 
#way 1
#Get the current time in mm-dd-yyyy format
from datetime import datetime
current_time = datetime.now().strftime('%m-%d-%y')
#print the current time


new_folder = "abfss://processed@smartpolicysystemadls.dfs.core.windows.net/PolicyData/"+current_time
dbutils.fs.mv("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/PolicyData/", new_folder,True)
