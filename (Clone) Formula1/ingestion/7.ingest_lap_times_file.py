# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step1 : Read the CSV file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
] )

# COMMAND ----------

lap_times_df = spark.read\
                .schema(lap_times_schema)\
                .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename column and add new columns
# MAGIC 1. Rename Driverid,raceId
# MAGIC 2.Add ingestion_date  with current_timestamp

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_df).withColumnRenamed("raceId","race_id")\
                                        .withColumnRenamed("driverId","driver_id")\
                                        .withColumn("file_date", lit(v_file_date))
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC #### step3: write to output to processed container  in parquet  format

# COMMAND ----------

# lap_times_final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# overwrite_partition(lap_times_final_df, 'f1_processed', 'time', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap and tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'time', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


