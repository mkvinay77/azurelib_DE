# Databricks notebook source
# MAGIC %md
# MAGIC #ClaimData ingestion, new column, Delta table creation, move the data from the landing to processed 

# COMMAND ----------

customer_schema= "customer_id int,first_name string,last_name string,email string,phone string,country string,city string,registration_date timestamp,date_of_birth timestamp,gender string"
from pyspark.sql.functions import *
df = spark.read.csv("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/Customer/*.csv",inferSchema=False,schema=customer_schema,header=True)
df.show()

# COMMAND ----------

#adding merge_flag column
from pyspark.sql.functions import lit
df_merge_falg = df.withColumn("merge_falg", lit(False))
display(df_merge_falg)

# COMMAND ----------

#Creating the table in bronzelayer
df_merge_falg.write.mode("append").option("path","abfss://bronzelayer@smartpolicysystemadls.dfs.core.windows.net/CustomerData/").saveAsTable("bronzelayer.Customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronzelayer.Customer

# COMMAND ----------

#Now moving the data from landing to processed 
# to get current date 
from datetime import datetime

#will use function this time
def getFilePathWithDates(filePath):
    #get the current time in mm-dd-yyyy format
    current_time = datetime.now().strftime('%m-%d-%y')
    new_file_path = filePath+'/'+current_time
    return new_file_path

# COMMAND ----------

dbutils.fs.mv("abfss://landing@smartpolicysystemadls.dfs.core.windows.net/Customer/",getFilePathWithDates("abfss://processed@smartpolicysystemadls.dfs.core.windows.net/CustomerData/"),True)
