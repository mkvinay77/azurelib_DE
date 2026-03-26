# Databricks notebook source
# MAGIC %md
# MAGIC ### 1) Remove all where claim_id,polioci_id, claim_status,claim_amount,lastupdate IS NULL
# MAGIC ### 2) Remove all rows where policy_id ID NOT exist in policy table
# MAGIC ### 3) Convert the date_of_claim to Date column with formate(MM-dd-yyyy)
# MAGIC ### 4) Ensure claim amount is > 0

# COMMAND ----------

# df = spark.sql("SELECT * FROM bronzelayer.claim AS c WHERE c.claim_id IS NOT NULL AND c.claim_amount IS NOT NULL AND c.policy_id IS NOT NULL AND c.claim_status IS NOT NULL AND c.LastUpdatedTimeStamp IS NOT NULL AND c.merge_flag=FALSE")
# display(df)

# COMMAND ----------

# df = spark.sql("SELECT c.* FROM bronzelayer.claim AS c INNER JOIN bronzelayer.policy AS p ON c.policy_id=p.policy_id WHERE c.claim_id IS NOT NULL AND c.claim_amount IS NOT NULL AND c.policy_id IS NOT NULL AND c.claim_status IS NOT NULL AND c.LastUpdatedTimeStamp IS NOT NULL AND c.merge_flag=FALSE")
# display(df)

# COMMAND ----------

# df = spark.sql("SELECT c.claim_id, c.claim_amount, c.LastUpdatedTimeStamp, c.policy_id, c.claim_status, date_format(c.date_of_claim,'MM-dd-yyyy' ) AS date_of_claim, c.merge_flag FROM bronzelayer.claim AS c INNER JOIN bronzelayer.policy AS p ON c.policy_id=p.policy_id WHERE c.claim_id IS NOT NULL AND c.claim_amount IS NOT NULL AND c.policy_id IS NOT NULL AND c.claim_status IS NOT NULL AND c.LastUpdatedTimeStamp IS NOT NULL AND c.merge_flag=FALSE")
# display(df)

# COMMAND ----------

#Ensure claim amount is > 0

# COMMAND ----------

df = spark.sql("SELECT c.claim_id, c.claim_amount, c.LastUpdatedTimeStamp, c.policy_id, c.claim_status, to_date(date_format(c.date_of_claim,'MM-dd-yyyy'), 'MM-dd-yyyy') AS date_of_claim, c.merge_flag FROM bronzelayer.claim AS c INNER JOIN bronzelayer.policy AS p ON c.policy_id=p.policy_id WHERE c.claim_id IS NOT NULL AND c.claim_amount IS NOT NULL AND c.policy_id IS NOT NULL AND c.claim_status IS NOT NULL AND c.LastUpdatedTimeStamp IS NOT NULL AND c.merge_flag=FALSE AND c.claim_amount > 0")
display(df)

#to_date(date_format(c.date_of_claim,'MM-dd-yyyy'), 'MM-dd-yyyy') = to covert string to date type

# COMMAND ----------

#Merging the Claim table

# COMMAND ----------

df.createOrReplaceTempView('clean_claim')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silverlayer.Claim AS T USING clean_claim AS S ON
# MAGIC   t.claim_id = s.claim_id WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       t.policy_id = s.policy_id,
# MAGIC       t.date_of_claim = s.date_of_claim,
# MAGIC       t.claim_amount = s.claim_amount,
# MAGIC       t.claim_status = s.claim_status,
# MAGIC       t.LastUpdatedTimeStamp = s.LastUpdatedTimeStamp,
# MAGIC       t.merege_timestamp = current_timestamp()
# MAGIC   WHEN NOT MATCHED THEN 
# MAGIC   INSERT (claim_id,policy_id,date_of_claim,claim_amount,claim_status,LastUpdatedTimeStamp,merege_timestamp)
# MAGIC   VALUES(s.claim_id, s.policy_id, s.date_of_claim, s.claim_amount, s.claim_status, s.LastUpdatedTimeStamp,current_timestamp())

# COMMAND ----------

spark.sql("UPDATE bronzelayer.claim SET merge_flag = true WHERE merge_flag = false")
