# Databricks notebook source
# MAGIC %md
# MAGIC #Reading the Agent Data from ADLS loaction.

# COMMAND ----------

from pyspark.sql.functions import *
schema ="agent_id integer,agent_name string,agent_email string,agent_phone string,branch_id integer,create_timestamp timestamp"
#No need to USE SCHEMA beacuse it is parquet file formate.
df=spark.read.parquet("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/AgentData/*.parquet",header=True)
df.show()

# COMMAND ----------

#Creating the extra flag column for future records updates
df_with_flag =df.withColumn("merge_flag",lit(False))
#df_with_flag.display()
spark.sql("CREATE SCHEMA IF NOT EXISTS bronzelayer")  #To create schema 
df_with_flag.write.mode("append").option("path","abfss://bronzelayer@smartpolicysystemadls.dfs.core.windows.net/AgentData/").saveAsTable("bronzelayer.Agent")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronzelayer.Agent

# COMMAND ----------

#Now moving the data from landing to processed container 
#way 1
#Get the current time in mm-dd-yyyy format
from datetime import datetime
current_time = datetime.now().strftime('%m-%d-%y')
#print the current time


new_folder = "abfss://processed@smartpolicysystemadls.dfs.core.windows.net/AgentData/"+current_time
dbutils.fs.mv("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/AgentData/", new_folder,True )

# COMMAND ----------

#To check the processed data path
df2 = spark.read.parquet("abfss://processed@smartpolicysystemadls.dfs.core.windows.net/AgentData/03-20-26/*.parquet",headder=True)
display(df2)

# COMMAND ----------

#way 2

from datetime import datetime

# 1. Setup paths
current_time = datetime.now().strftime("%Y-%m-%d")
source_dir = "abfss://landing@smartpolicysystemadls.dfs.core.windows.net/AgentData/"
new_folder = f"abfss://processed@smartpolicysystemadls.dfs.core.windows.net/Agent/{current_time}/"

# 2. Ensure the destination folder exists (Important!)
dbutils.fs.mkdirs(new_folder)

# 3. List all files inside the source
files_to_move = dbutils.fs.ls(source_dir)

# 4. Loop and move each file individually
for f in files_to_move:
    # This moves the file and keeps 'Agent' as an empty folder
    dbutils.fs.mv(f.path, new_folder + f.name, recurse=True)
    print(f"Moved: {f.name} to {new_folder}")

print(f"Success! {len(files_to_move)} files moved. 'Agent' folder is still there.")
