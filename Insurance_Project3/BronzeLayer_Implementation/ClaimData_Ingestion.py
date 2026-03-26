# Databricks notebook source
from pyspark.sql.functions import *
#Defining the Schema using declaritive manner
claim_schema ="claim_id int,policy_id int,date_of_claim timestamp,claim_amount double,claim_status string,LastUpdatedTimeStamp timestamp"

df = spark.read.parquet("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/ClaimData/*.parquet",inferSchema=False,schema=claim_schema,header=True)
df.show()



# COMMAND ----------

#Creatind addtional column
from pyspark.sql.functions import *
df_merge_flag = df.withColumn("merge_flag",lit(False))
df_merge_flag.show()

# COMMAND ----------

#Write data to bronzelayer 
df_merge_flag.write.option("path","abfss://bronzelayer@smartpolicysystemadls.dfs.core.windows.net/ClaimData/").mode("append").saveAsTable("bronzelayer.Claim")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronzelayer.claim

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
dbutils.fs.mv("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/ClaimData/", getFilePathWithDates("abfss://processed@smartpolicysystemadls.dfs.core.windows.net/ClaimData/"),True)
