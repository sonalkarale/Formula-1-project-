# Databricks notebook source
# MAGIC %md 
# MAGIC ###Ingest races.csv file 

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date") 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1: Read the csv file using spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType,StringType,DateType

# COMMAND ----------

races_schema = StructType(fields =[StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",IntegerType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("date",DateType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("url",StringType(),True)
                                   
])

# COMMAND ----------

races_df = spark.read\
                .option("header",True)\
                .schema(races_schema)\
                .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step2: Add Ingestion Date and race timestamp to the dataframe 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,lit,col,concat

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df)\
                            .withColumn("file_date",lit(v_file_date))\
                            .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step3: Select only columns requried and rename as required

# COMMAND ----------

races_select_df = races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step4: Write the output to processed containers in parquet format

# COMMAND ----------

races_select_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####use partition by 

# COMMAND ----------

# races_select_df.write.mode("overwrite").partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# COMMAND ----------


