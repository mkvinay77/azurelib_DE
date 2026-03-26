# Databricks notebook source
from pyspark.sql.functions import *
schema ="branch_id int, branch_country string, branch_city string"

df=spark.read.parquet("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/BranchData/*.parquet",inferSchema=False,schema=schema,header=True)
#df.show()
#Creating the new column
df_merge_flag = df.withColumn("merge_flag", lit(False))
#df_merge_flag.show()
# will create the table in bronzelayer for branch data
df_merge_flag.write.mode("append").option("path","abfss://bronzelayer@smartpolicysystemadls.dfs.core.windows.net/BranchData/").saveAsTable("bronzelayer.Branch")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronzelayer.Branch

# COMMAND ----------

# to get current date 
from datetime import datetime

#will use function this time
def getFilePathWithDates(filePath):
    #get the current time in mm-dd-yyyy format
    current_time = datetime.now().strftime('%m-%d-%y')
    new_file_path = filePath+'/'+current_time
    return new_file_path

# COMMAND ----------

#moving the data from landing container to processed container
dbutils.fs.mv("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/BranchData/", getFilePathWithDates("abfss://processed@smartpolicysystemadls.dfs.core.windows.net/BranchData/"),True)
