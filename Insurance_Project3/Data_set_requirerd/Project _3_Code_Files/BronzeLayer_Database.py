# Databricks notebook source
# MAGIC %md
# MAGIC <b> Create BronzeLayer Database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new database
# MAGIC CREATE DATABASE IF NOT EXISTS bronzelayer;
# MAGIC
# MAGIC -- Use the created database
# MAGIC USE bronzelayer;

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create and Load Agent Table in bronze layer

# COMMAND ----------

from pyspark.sql.functions import lit

schema = "agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp"
df = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).parquet("/mnt/landing/AgentData/*.parquet")
df_with_flag = df.withColumn("merge_flag", lit(False))
df_with_flag.show()
#df_with_flag.write.format("delta").option("path","/mnt/bronzelayer/agent").mode("append").saveAsTable("bronzelayer.Agent")


# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check Agent table in bronzelayer

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC select * from bronzelayer.agent

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Move the Agent input file from landing container to processed container

# COMMAND ----------

#dbutils.fs.help()
dbutils.fs.mv('/mnt/landing/Test/', '/mnt/processed/TestAgent', True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Test Case

# COMMAND ----------

from pyspark.sql.functions import isnull
def test_no_null_values(df):
    # Check for null values in all columns
    null_count = spark.sql("select * from agent where agent_name='' ").count()

    # Assert that there are no null values
    assert null_count > 0, f"There are {null_count} null values in the DataFrame"

test_no_null_values(df)
