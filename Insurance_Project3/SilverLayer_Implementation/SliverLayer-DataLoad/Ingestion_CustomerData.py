# Databricks notebook source
# MAGIC %md
# MAGIC ### 1st Remove all records where customer_id NOT NULL.
# MAGIC ### 2nd Remove records where Gender is other than Male/Famale.

# COMMAND ----------

df = spark.sql("SELECT * FROM bronzelayer.customer WHERE customer_id IS NOT NULL AND gender IN ('Male','Female') ")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3rd Outlier check at some registration_date > DOB

# COMMAND ----------

df = spark.sql("SELECT * FROM bronzelayer.customer WHERE customer_id IS NOT NULL AND gender IN ('Male','Female') AND registration_date > date_of_birth ")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge the data into the silver layer while adding the current timestamp

# COMMAND ----------

df.createOrReplaceTempView("clean_customer")
spark.sql("MERGE INTO silverlayer.customer AS T USING clean_customer AS S ON t.customer_id = s.customer_id WHEN MATCHED THEN UPDATE SET T.city = S.city, T.email=S.email, T.phone=S.phone, T.gender=S.gender, T.country = S.country, T.first_name=S.first_name, T.last_name=S.last_name, T.date_of_birth=S.date_of_birth, T.registration_date=S.registration_date, T.merged_timestamp = current_timestamp() WHEN NOT MATCHED THEN INSERT ( customer_id, first_name, last_name, email, phone, country, city, registration_date, date_of_birth, gender, merged_timestamp ) VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.phone, s.country, s.city, s.registration_date, s.date_of_birth, s.gender,current_timestamp())")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverlayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update the flag in the bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronzelayer.customer SET merge_falg = true WHERE merge_falg = false 
