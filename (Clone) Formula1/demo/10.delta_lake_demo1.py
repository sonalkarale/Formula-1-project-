# Databricks notebook source
# MAGIC %md 
# MAGIC 1. write data to delta lake(managed table)
# MAGIC 2. write data to delta lake(external table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo1
# MAGIC location '/mnt/formula1datalake24/demo1'

# COMMAND ----------

results_df = spark.read \
.option("inferSchema" ,True)\
.json("/mnt/formula1datalake24/raw/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_demo1.results_mananged

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo1.results_managed")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo1.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1datalake24/demo1/results_external")

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table f1_demo1.results_external

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table f1_demo1.results_external
# MAGIC using delta
# MAGIC location '/mnt/formula1datalake24/demo1/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo1.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').load("/mnt/formula1datalake24/demo1/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo1.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo1.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###1. update delta table
# MAGIC ###2.delete from delta table 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo1.results_managed

# COMMAND ----------

# MAGIC %sql 
# MAGIC update f1_demo1.results_managed
# MAGIC   set points = 11- position
# MAGIC   where position <= 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo1.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1datalake24/demo1/results_managed')

deltaTable.update(
    condition = "position <= 10",
    set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo1.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1datalake24/demo1/results_managed')

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### upsert using merge 
# MAGIC in 10.1delta_lake_demo1
