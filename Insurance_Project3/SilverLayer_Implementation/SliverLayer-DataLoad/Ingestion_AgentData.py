# Databricks notebook source
df = spark.sql("SELECT * FROM bronzelayer.Agent WHERE merge_flag = false")

# COMMAND ----------

# MAGIC %md
# MAGIC 1st Transformation
# MAGIC Remove all the rows where Branch ID not exist in Branch Table 

# COMMAND ----------

df_branch = spark.sql("SELECT * FROM bronzelayer.Branch")

#We have to use the JOIN
df_result = spark.sql("SELECT * FROM bronzelayer.agent INNER JOIN bronzelayer.branch ON agent.branch_id = branch.branch_id WHERE agent.merge_flag = false")
display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC 2nd Transformation: 
# MAGIC Ensure all the phone have valid 10 Digit Phone number

# COMMAND ----------

#1st way 
df = spark.sql("SELECT a.* FROM bronzelayer.agent AS a INNER JOIN bronzelayer.branch AS b ON a.branch_id = b.branch_id WHERE a.merge_flag = false AND length(agent_phone)=10")
display(df)

#2nd way
# from pyspark.sql.functions import *
# df_phone = df_result.filter(length(col("agent_phone")) == 10)
# display(df_phone)

# COMMAND ----------

# MAGIC %md 
# MAGIC 3rd Transformation:
# MAGIC Replace all the NULL email with 'admin@azurelib.com'

# COMMAND ----------

# df_email = df_phone.fillna({'agent_email':'admin@azurelib.com'})
# display(df_email)

# COMMAND ----------

# %sql
# SELECT * FROM bronzelayer.agent WHERE agent_email=''

# COMMAND ----------

#Fill will the empty email
df.createOrReplaceTempView("agent_temp")
df_email = spark.sql("SELECT a.agent_id, a.agent_name, a.agent_phone, a.branch_id, a.create_timestamp, regexp_replace(a.agent_email, '', 'admin@azurelib.com') AS agent_email FROM agent_temp AS a WHERE a.agent_email = '' UNION SELECT a.agent_id, a.agent_name, a.agent_phone, a.branch_id, a.create_timestamp, a.agent_email FROM agent_temp AS a WHERE a.agent_email !='' ")
display(df_email)





# COMMAND ----------

# MAGIC %md
# MAGIC add the merged_timestamp(current timestamp)

# COMMAND ----------

#1st way
# date_final = df_email.withColumn("merged_timestamp",current_timestamp())
# display(date_final)

# COMMAND ----------

#2nd way
df_email.createOrReplaceTempView("clean_agent")

spark.sql("MERGE INTO silverlayer.agent AS T USING clean_agent AS S ON T.agent_id = S.agent_id WHEN MATCHED THEN UPDATE SET T.agent_phone = S.agent_phone, T.agent_email = S.agent_email, T.agent_name = S.agent_name, T.branch_id = S.branch_id, T.create_timestamp = S.create_timestamp, T.merged_timestamp = current_timestamp() WHEN NOT MATCHED THEN INSERT (agent_id, agent_name, agent_phone, branch_id, create_timestamp, agent_email, merged_timestamp) VALUES (S.agent_id, S.agent_name, S.agent_phone, S.branch_id, S.create_timestamp, S.agent_email, current_timestamp())")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.agent

# COMMAND ----------

# MAGIC %md
# MAGIC add the merge_flag = True of the bronze layer

# COMMAND ----------

spark.sql("update bronzelayer.agent set merge_flag = True WHERE merge_flag = false")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronzelayer.agent
