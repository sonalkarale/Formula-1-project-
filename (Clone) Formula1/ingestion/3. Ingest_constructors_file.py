# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date") 

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1: Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
                      .schema(constructors_schema)\
                      .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step2 : Drop unwanted columns from dataframe

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

constructor_droped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step3 : Rename columns  and add ingestion date
# MAGIC

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_droped_df).withColumnRenamed("constructorId","constructor_id")\
                                            .withColumnRenamed("constructorRef","constructor_ref")\
                                            .withColumn("file_date",lit(v_file_date))
                                            

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step4: Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
