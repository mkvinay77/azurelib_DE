# Databricks notebook source
# MAGIC %md
# MAGIC #1st Remove all Where branch_id not null
# MAGIC #2nd Remove all the leading and trailing space in Branch Country and Convert it into UPPER CASE

# COMMAND ----------

df = spark.sql("SELECT b.branch_id,b.branch_city, UPPER(trim(b.branch_country)) AS branch_country FROM bronzelayer.branch AS b WHERE b.branch_id IS NOT NULL AND b.merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #3rd Merge into Sliver layer table

# COMMAND ----------

df.createOrReplaceTempView("clean_branch")
spark.sql("MERGE INTO silverlayer.branch AS T USING clean_branch AS S ON t.branch_id = s.branch_id WHEN MATCHED THEN UPDATE SET t.branch_country = s.branch_country, t.branch_city = s.branch_city, t.merged_timestamp = current_timestamp() WHEN NOT MATCHED THEN INSERT ( branch_id, branch_country,branch_city, merged_timestamp) VALUES (s.branch_id, s.branch_country, s.branch_city, current_timestamp())")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.branch

# COMMAND ----------

#Update the bronzelayey merge_flag = true

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronzelayer.branch SET merge_flag = true WHERE merge_flag = false
