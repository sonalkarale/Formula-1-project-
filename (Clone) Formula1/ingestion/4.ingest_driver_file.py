# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest driver.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date") 

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 : Read the JSON  file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField,StructType, IntegerType, StringType,DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                   StructField("driverRef",StringType(),True),
                                   StructField("number",IntegerType(),True),
                                   StructField("code",StringType(),True),
                                   StructField("name",name_schema),
                                   StructField("dob",DateType(),True),
                                   StructField("nationality",StringType(),True),
                                   StructField("url",StringType(),True)
])

# COMMAND ----------

driver_df = spark.read\
                .schema(driver_schema)\
                .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step2: Rename columns and add new columns 
# MAGIC
# MAGIC 1. driverid renamed to driver_id
# MAGIC 2. driverref renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname 

# COMMAND ----------

from pyspark.sql.functions import col, concat,current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df = add_ingestion_date(driver_df).withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("file_date",lit(v_file_date))\
                                .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))
                                

# COMMAND ----------

# MAGIC %md
# MAGIC Step3: Drop the unwanted columns
# MAGIC  url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4: Write a output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------


