# Databricks notebook source
# MAGIC %md
# MAGIC <span style="color:orange">
# MAGIC  <h2> Live Project Workshop By Deepak Goyal - Azurelib Academy
# MAGIC </span>
# MAGIC   <h5>
# MAGIC     <span style="color:red">
# MAGIC <b>Author: Deepak Goyal <br>
# MAGIC    <a href='https://ade.azurelib.com'> ade.azurelib.com </a><br>
# MAGIC    Email at: admin@azurelib.com
# MAGIC    <br><a href ="https://www.linkedin.com/in/deepak-goyal-93805a17/" > Deepak Goyal Linkedin </a>
# MAGIC </span>

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create SilverLayer Database

# COMMAND ----------

#Write your code here

# COMMAND ----------

# MAGIC %md <b>Ensure this test case get pass

# COMMAND ----------

def test_create_database():
  databases = spark.sql("SHOW DATABASES LIKE 'silverlayer'")
  assert databases.count() == 0, "Database was not created"
  
test_create_database()

# COMMAND ----------

# MAGIC %md
# MAGIC <b> For any help reach out here:
# MAGIC  <a href="https://adeus.azurelib.com"> adeus.azurelib.com </a><br>
# MAGIC    Email at: admin@azurelib.com
# MAGIC    <br> 
# MAGIC    <a href ="https://www.linkedin.com/in/deepak-goyal-93805a17/" > Message Deepak Goyal here on Linkedin </a>
