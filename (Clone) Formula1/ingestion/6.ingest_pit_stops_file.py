# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stope.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step1 : Read the Json file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
] )

# COMMAND ----------

pit_stops_df = spark.read\
                .schema(pit_stops_schema)\
                    .option("multiline",True)\
                    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename column and add new columns
# MAGIC 1. Rename Driverid,raceId
# MAGIC 2.Add ingestion_date  with current_timestamp

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

# pit_stops_with_column_df = add_ingestion_date(pit_stops_df).withColumnRenamed("driverId","driver_id")\
#                                         .withColumnRenamed("raceId","race_id")\
#                                         .withColumn("data_source",lit(v_data_source))\
#                                         .withColumn("file_date", lit(v_file_date))
                                        

# COMMAND ----------

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("stop","pit_stops")\
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(pit_stops_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step3: write to output to processed container  in parquet  format

# COMMAND ----------

# final_df.write.mode("append").format("delta").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 4: Write output to proccessed container in parquet format

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.stops;

# COMMAND ----------


