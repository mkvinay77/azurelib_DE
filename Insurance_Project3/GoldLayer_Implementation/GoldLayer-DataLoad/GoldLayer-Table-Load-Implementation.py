# Databricks notebook source
# MAGIC %md
# MAGIC **Sales By Policy Type and Month:**
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance different policy types over time.

# COMMAND ----------

# For referenece
# CREATE OR REPLACE TABLE goldlayer.sales_by_policy_type_and_month(
#   policy_type STRING,
#   sale_month STRING,
#   total_premium INTEGER,
#   updated_timestamp TIMESTAMP
#   ) USING DELTA LOCATION "abfss://goldlayer@smartpolicysystemadls.dfs.core.windows.net/sales_by_policy_type_and_month"

# COMMAND ----------

# %sql 
# SELECT * FROM silverlayer.policy;



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_gold_sales_by_policy_type_and_month AS 
# MAGIC SELECT 
# MAGIC policy_type,
# MAGIC month(start_date) AS sale_month,
# MAGIC -- COUNT(month(start_date)) AS sales_per_month, 
# MAGIC SUM(premium) AS total_premium 
# MAGIC FROM bronzelayer.policy 
# MAGIC GROUP BY policy_type, month(start_date)
# MAGIC HAVING policy_type IS NOT NULL
# MAGIC ORDER BY policy_type, sale_month desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_gold_sales_by_policy_type_and_month

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO goldlayer.sales_by_policy_type_and_month AS T USING vw_gold_sales_by_policy_type_and_month AS S ON t.policy_type = s.policy_type
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET
# MAGIC    T.sale_month = S.sale_month, T.total_premium = S.total_premium, T.updated_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC  INSERT(
# MAGIC   policy_type, sale_month, total_premium, updated_timestamp
# MAGIC  )
# MAGIC  VALUES(
# MAGIC   s.policy_type,
# MAGIC   s.sale_month,
# MAGIC   s.total_premium,
# MAGIC   current_timestamp()
# MAGIC  )
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM goldlayer.sales_by_policy_type_and_month

# COMMAND ----------

# MAGIC %md
# MAGIC **Claims By Policy Type and Status:** This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claim process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_gold_claims_by_policy_type_and_status
# MAGIC AS
# MAGIC SELECT
# MAGIC     policy_type,
# MAGIC     claim_status,
# MAGIC     COUNT(*) AS total_claims,
# MAGIC     SUM(claim_amount) AS total_claim_amount
# MAGIC FROM
# MAGIC     silverlayer.claim AS C
# MAGIC     JOIN silverlayer.policy AS p
# MAGIC         ON c.policy_id = p.policy_id
# MAGIC GROUP BY
# MAGIC     policy_type,
# MAGIC     claim_status HAVING p.policy_type is NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_gold_claims_by_policy_type_and_status

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO goldlayer.claims_by_policy_type_and_status AS T USING vw_gold_claims_by_policy_type_and_status AS S ON t.policy_type = s.policy_type
# MAGIC AND t.claim_status = s.claim_status WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   T.total_claim_amount = s.total_claim_amount, T.total_claims = s.total_claims, T.updated_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (
# MAGIC     policy_type, claim_status, total_claim_amount, total_claims, 
# MAGIC     updated_timestamp 
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     s.policy_type, 
# MAGIC     s.claim_status, 
# MAGIC     s.total_claim_amount, 
# MAGIC     s.total_claims, 
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM goldlayer.claims_by_policy_type_and_status

# COMMAND ----------

# MAGIC %md
# MAGIC **Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_gold_claims_analysis AS
# MAGIC SELECT
# MAGIC     policy_type,
# MAGIC     AVG(claim_amount) AS avg_claim_amount,
# MAGIC     MAX(claim_amount) AS max_claim_amount,
# MAGIC     MIN(claim_amount) AS min_claim_amount,
# MAGIC     COUNT(DISTINCT c.claim_id) AS total_claims
# MAGIC FROM
# MAGIC     silverlayer.claim AS c
# MAGIC     JOIN silverlayer.policy AS p
# MAGIC         ON c.policy_id = p.policy_id
# MAGIC GROUP BY
# MAGIC     policy_type HAVING p.policy_type IS NOT NULL;
# MAGIC
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_gold_claims_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO goldlayer.claims_analysis AS T USING vw_gold_claims_analysis AS S ON T.policy_type = S.policy_type 
# MAGIC  WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC    T.avg_claim_amount = S.avg_claim_amount, T.max_claim_amount = S.max_claim_amount, T.min_claim_amount = S.min_claim_amount, T.total_claims = S.total_claims, T.updated_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC  INSERT (
# MAGIC   policy_type, avg_claim_amount, max_claim_amount, min_claim_amount, total_claims,
# MAGIC   updated_timestamp
# MAGIC  )
# MAGIC VALUES
# MAGIC   (
# MAGIC   s.policy_type,
# MAGIC   s.avg_claim_amount,
# MAGIC   s.max_claim_amount,
# MAGIC   s.min_claim_amount, 
# MAGIC   s.total_claims,
# MAGIC   current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM goldlayer.claims_analysis
