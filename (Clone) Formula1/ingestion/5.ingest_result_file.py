# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest result.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step1: Read the JSON file using spark Dataframe API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql.functions import current_timestamp,lit, col

# COMMAND ----------

result_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                   StructField("raceId",IntegerType(),True),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("constructorId",IntegerType(),True),
                                   StructField("number",IntegerType(),True),
                                   StructField("grid",IntegerType(),True),
                                   StructField("position",IntegerType(),True),
                                   StructField("positionText",StringType(),True),
                                   StructField("positionOrder",IntegerType(),True),
                                   StructField("points",FloatType(),True),
                                   StructField("laps",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True),
                                   StructField("fasterLap",IntegerType(),True),
                                    StructField("rank",IntegerType(),True),
                                    StructField("fasterLapTime",StringType(),True),
                                    StructField("fasterLapSpeed",FloatType(),True),
                                     StructField("statusId",StringType(),True),

 ])

# COMMAND ----------

result_df = spark.read\
            .schema(result_schema)\
            .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step 2:  Rename column and add new column ingestion date 
# MAGIC

# COMMAND ----------

results_with_column_df = add_ingestion_date(result_df).withColumnRenamed("resultId","result_id")\
                                .withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("constructorId","constructor_id")\
                                .withColumnRenamed("positionText","position_text")\
                                .withColumnRenamed("positionOrder","position_order")\
                                .withColumnRenamed("fasterLap","faster_lap")\
                                  .withColumnRenamed("fasterLap","faster_lap")\
                                  .withColumnRenamed("fasterLapTime","faster_lap_time")\
                                  .withColumnRenamed("fasterLapSpeed","faster_lap_speed")\
                                  .withColumn("file_date",lit(v_file_date))
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: Drop unwanted columns
# MAGIC

# COMMAND ----------

result_final_df = results_with_column_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md 
# MAGIC De-Dupe dataframe 

# COMMAND ----------

results_deduped_df = result_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4: Write output  to proccessed container in parquet format
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 1

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP if EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# result_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.results

# COMMAND ----------

# overwrite_partition(result_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')


# COMMAND ----------

# result_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from f1_processed.results
# MAGIC -- where file_date  = '2021-03-21';

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_id, driver_id, count(1)  from f1_processed.results
# MAGIC group by race_id,driver_id
# MAGIC having count(1) > 1
# MAGIC order by race_id , driver_id desc
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from f1_processed.results
# MAGIC where race_id = 540 and driver_id = 229;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


