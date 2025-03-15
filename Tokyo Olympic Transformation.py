# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType,BooleanType,DataType

# COMMAND ----------

# MAGIC %md
# MAGIC Using app factory to get the information from the DataLake

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "<client_id>",
"fs.azure.account.oauth2.client.secret": '<clien_secret>',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory_id>/oauth2/token"}

dbutils.fs.mount(
source = "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net",
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC Need to give permission inorder to give access

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

spark

# COMMAND ----------

atheletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/Athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/Coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/EntriesGender.csv")
medal = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/Medals.csv")
teams = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/Teams.csv")

# COMMAND ----------

atheletes.show()

# COMMAND ----------

atheletes.printSchema()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender =entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

top_gold_medal_countries = medal.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

atheletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/atheletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medal.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")