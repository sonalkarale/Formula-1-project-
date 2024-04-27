# Databricks notebook source
# MAGIC %md 
# MAGIC ###
# MAGIC upsert using merge

# COMMAND ----------

from pyspark.sql.functions import upper 


# COMMAND ----------

drivers_day1_df = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/formula1datalake24/raw/2021-03-28/drivers.json")\
    .filter("driverId <= 10 ")\
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------


drivers_day2_df = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/formula1datalake24/raw/2021-03-28/drivers.json")\
    .filter("driverId between 6 and 15 ")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/formula1datalake24/raw/2021-03-28/drivers.json")\
    .filter("driverId between 1 and 5 or driverId between 16 and 20")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists  f1_demo1.drivers_merge (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename STRING,
# MAGIC surname string,
# MAGIC createDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo1.drivers_merge tgt using drivers_day1 upd on tgt.driverId = upd.driverId
# MAGIC when matched then
# MAGIC update
# MAGIC set
# MAGIC   tgt.dob = upd.dob,
# MAGIC   tgt.forename = upd.forename,
# MAGIC   tgt.surname = upd.surname,
# MAGIC   tgt.updatedDate = current_timestamp
# MAGIC   when not matched then
# MAGIC insert
# MAGIC   (driverId, dob, forename, surname, createDate)
# MAGIC values
# MAGIC   (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo1.drivers_merge tgt using drivers_day2 upd on tgt.driverId = upd.driverId
# MAGIC when matched then
# MAGIC update
# MAGIC set
# MAGIC   tgt.dob = upd.dob,
# MAGIC   tgt.forename = upd.forename,
# MAGIC   tgt.surname = upd.surname,
# MAGIC   tgt.updatedDate = current_timestamp
# MAGIC   when not matched then
# MAGIC insert
# MAGIC   (driverId, dob, forename, surname, createDate)
# MAGIC values
# MAGIC   (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable 

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1datalake24/demo1/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driversId == upd.driverId") \
    .whenMatchedUpdate(set={
        "dob": "upd.dob", 
        "forename": "upd.forename", 
        "surname": "upd.surname", 
        "updatedDate": current_timestamp()
    }) \
    .whenNotMatchedInsert(values={
        # Define the dictionary for values to insert, for example:
        # "driversId": "upd.driverId",
        # "dob": "upd.dob",
        # ...
    })

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1datalake24/demo1/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History and Versioning 
# MAGIC 2. Time Travel
# MAGIC 3. vaccum

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history f1_demo1.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge version as of 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge timestamp as of "2024-04-15T08:41:58.000+00:00" 

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf","2024-04-15T08:41:58.000+00:00").load("/mnt/formula1datalake24/demo1/drivers_merge")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo1.drivers_merge Retain 0 hours

# COMMAND ----------

# %sql
# select * from f1_demo1.drivers_merge timestamp as of "2024-04-15T08:41:58.000+00:00" 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo1.drivers_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from f1_demo1.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo1.drivers_merge version as of 4;

# COMMAND ----------

# %sql 
# merge into f1_demo1.drivers_merge tgt
# using f1_demo.drivers_merge version AS of 2 src
# on (tgt.driverId = src.driverId)
# when not matched then 
# insert *

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists  f1_demo1.drivers_txn (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename STRING,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo1.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo1.drivers_txn
# MAGIC select * from f1_demo1.drivers_merge
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo1.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo1.drivers_txn
# MAGIC select * from f1_demo1.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo1.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from f1_demo1.drivers_txn
# MAGIC where driverId = 1

# COMMAND ----------

# MAGIC %md 
# MAGIC convert parquet into delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists  f1_demo1.drivers_converts_to_delta (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename STRING,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo1.drivers_converts_to_delta
# MAGIC select * from f1_demo1.drivers_merge
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo1.drivers_converts_to_delta

# COMMAND ----------

df = spark.table("f1_demo1.drivers_converts_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1datalake24/demo1/drivers_converts_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/formula1datalake24/demo1/drivers_converts_to_delta_new`

# COMMAND ----------


