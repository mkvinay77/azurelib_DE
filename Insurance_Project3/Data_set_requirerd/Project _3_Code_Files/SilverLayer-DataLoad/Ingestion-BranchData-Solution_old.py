# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where brnach_id not null
# MAGIC

# COMMAND ----------

df = spark.sql("SELECT * from bronzelayer.branch where branch_id is not null and merge_flag = FALSE")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all the leading and trailing spaces in Brnach Country and covert it into UPPER CASE

# COMMAND ----------

df = spark.sql("SELECT branch_id, branch_city, upper(trim(branch_country)) from bronzelayer.branch where branch_id is not null and merge_flag = FALSE")
display(df)
df.createOrReplaceTempView("branch_stg")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronzelayer.branchcopy as  select * from bronzelayer.branch

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge into Silver layer table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO bronzelayer.branchcopy AS t
# MAGIC USING bronzelayer.branch AS s
# MAGIC ON t.branch_id   = s.branch_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET t.branch_city = s.branch_city,  t.branch_country = s.branch_country
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (branch_id, branch_city, branch_country) VALUES (s.branch_id, s.branch_city, s.branch_country)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md <b>
# MAGIC Update the merged_flag in the bronzelayer table

# COMMAND ----------

spark.sql ("update bronzelayer.branchcopy set merge_flag = True")
display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.branchcopy
