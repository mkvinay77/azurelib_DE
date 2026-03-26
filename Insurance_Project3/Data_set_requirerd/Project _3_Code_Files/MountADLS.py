# Databricks notebook source
# MAGIC %md
# MAGIC <b> Mount the Bronze Layer Container:
# MAGIC   

# COMMAND ----------

# Container name
# Stoage account name
# Storage account SAS token
# Mount point name (it could be anything /mnt/.....)

#code Template


dbutils.fs.mount( source = 'wasbs://bronzelayer@policysystemadls.blob.core.windows.net', 
                 mount_point= '/mnt/bronzelayer', extra_configs ={'fs.azure.sas.bronzelayer.policysystemadls.blob.core.windows.net':'?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-05T22:11:54Z&st=2023-04-17T14:11:54Z&spr=https&sig=2J%2BKZHI5Bc%2Bzin1m8H7wEXtV%2FRWdhVx8VYqve%2Ft9070%3D'})



# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking Bronze Layer Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronzelayer

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mount the Landing Container:

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://landing@policysystemadls.blob.core.windows.net', 
                 mount_point= '/mnt/landing', extra_configs ={'fs.azure.sas.landing.policysystemadls.blob.core.windows.net':'?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-05T22:11:54Z&st=2023-04-17T14:11:54Z&spr=https&sig=2J%2BKZHI5Bc%2Bzin1m8H7wEXtV%2FRWdhVx8VYqve%2Ft9070%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking landing Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/landing

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mount the Processed Container:

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://processed@policysystemadls.blob.core.windows.net', 
                 mount_point= '/mnt/processed', extra_configs ={'fs.azure.sas.processed.policysystemadls.blob.core.windows.net':'?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-05T22:11:54Z&st=2023-04-17T14:11:54Z&spr=https&sig=2J%2BKZHI5Bc%2Bzin1m8H7wEXtV%2FRWdhVx8VYqve%2Ft9070%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking processed Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create Silver Layer Mount Point

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://silverlayer@policysystemadls.blob.core.windows.net', 
                 mount_point= '/mnt/silverlayer', extra_configs ={'fs.azure.sas.silverlayer.policysystemadls.blob.core.windows.net':'?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-05T22:11:54Z&st=2023-04-17T14:11:54Z&spr=https&sig=2J%2BKZHI5Bc%2Bzin1m8H7wEXtV%2FRWdhVx8VYqve%2Ft9070%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check Silver Layer Mount Point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silverlayer

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create Golder Layer Mount Point

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check Golden Layer Mount Point

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create and Load Agent Table in bronze layer

# COMMAND ----------

schema = "agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp"
df = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).parquet("/mnt/landing/AgentData/*.parquet")
df.write.format("delta").option("path","/mnt/bronzelayer/a2").mode("append").saveAsTable("Agent")



# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check Agent table in bronzelayer

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC select * from agent where agent_name=""

# COMMAND ----------

from pyspark.sql.functions import isnull
def test_no_null_values(df):
    # Check for null values in all columns
    null_count = spark.sql("select * from agent where agent_name='' ").count()

    # Assert that there are no null values
    assert null_count > 0, f"There are {null_count} null values in the DataFrame"

test_no_null_values(df)
