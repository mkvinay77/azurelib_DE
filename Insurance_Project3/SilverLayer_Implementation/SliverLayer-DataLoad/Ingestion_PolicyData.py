# Databricks notebook source
# MAGIC %md
# MAGIC ###1 REMOVE all the rows where customer id,Policy ID is NULL
# MAGIC ###2 REMOVE all the rows where customer ID not exist in Customer table
# MAGIC ###3 Remove all eows where Customer ID not exist in customer table
# MAGIC
# MAGIC

# COMMAND ----------

# df = spark.sql("SELECT * FROM bronzelayer.policy WHERE policy_id IS NOT NULL AND customer_id IS NOT NULL AND merge_falg= FALSE")
# display(df)

# COMMAND ----------

### Remove all rows where Customer ID not exist in customer table

# COMMAND ----------

# df = spark.sql("SELECT p.* FROM bronzelayer.policy AS p INNER JOIN bronzelayer.customer AS c ON p.customer_id=c.customer_id WHERE p.policy_id IS NOT NULL AND p.customer_id IS NOT NULL AND p.merge_falg= FALSE")
# display(df)

# COMMAND ----------

#4 Every policy must have premimum and coverage amount > 0

# df = spark.sql("SELECT p.* FROM bronzelayer.policy AS p INNER JOIN bronzelayer.customer AS c ON p.customer_id=c.customer_id WHERE p.policy_id IS NOT NULL AND p.customer_id IS NOT NULL AND p.merge_falg= FALSE AND p.premium > 0 AND p.coverage_amount > 0 ")
# display(df)

# COMMAND ----------

#5 Validate end_date > start_date

# COMMAND ----------

df = spark.sql("SELECT p.* FROM bronzelayer.policy AS p INNER JOIN bronzelayer.customer AS c ON p.customer_id=c.customer_id WHERE p.policy_id IS NOT NULL AND p.customer_id IS NOT NULL AND p.merge_falg= FALSE AND p.premium > 0 AND p.coverage_amount > 0 AND p.end_date > p.start_date")
display(df)

# COMMAND ----------

# Merge the table with merged_date_timestamp as current timestamp

# COMMAND ----------

df.createOrReplaceTempView('clean_policy')
spark.sql("MERGE INTO silverlayer.policy AS T USING clean_policy AS S ON t.policy_id=s.policy_id WHEN MATCHED THEN UPDATE SET t.policy_id = s.policy_id, t.policy_type = s.policy_type, t.customer_id = s.customer_id, t.start_date = s.start_date, t.end_date = s.end_date, t.premium = s.premium, t.coverage_amount = s.coverage_amount, t.merge_timestamp= current_timestamp() WHEN NOT MATCHED THEN INSERT (policy_id, policy_type, customer_id, start_date, end_date, premium, coverage_amount, merge_timestamp) VALUES (s.policy_id, s.policy_type, s.customer_id, s.start_date, s.end_date, s.premium, s.coverage_amount, current_timestamp())")


# COMMAND ----------

# MAGIC %md
# MAGIC Update the merged_falg in broze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronzelayer.policy SET merge_falg = true WHERE merge_falg = false
