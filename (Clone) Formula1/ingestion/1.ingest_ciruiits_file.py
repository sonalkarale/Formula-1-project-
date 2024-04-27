# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Ciruits.csv file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date") 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Step1 : Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------

curcuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField("circuitRef",StringType(), True),
                                     StructField("name",StringType(), True),
                                     StructField("location",StringType(), True),
                                     StructField("country",StringType(), True),
                                     StructField("lat",DoubleType(), True),
                                     StructField("alt",IntegerType(), True),
                                     StructField("url",StringType(),True),
           
])

# COMMAND ----------

circuits_df=spark.read\
    .option("header",True)\
    .schema(curcuits_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
            

# COMMAND ----------

# MAGIC %md 
# MAGIC Select Only the Required Columns

# COMMAND ----------

curcuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","alt")

# COMMAND ----------

# MAGIC %md
# MAGIC Rename a columns as Per required

# COMMAND ----------

curcuits_renamed_df = curcuits_selected_df.withColumnRenamed("curcuitId","curcuits_id")\
                                        .withColumnRenamed("curcuitRef","curcuit_ref")\
                                        .withColumnRenamed("lat","lattitude")\
                                        .withColumnRenamed("lng","longitude")\
                                        .withColumnRenamed("alt","altitude")\
                                        .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Adding a (new column) Ingestion date to the dataframe

# COMMAND ----------

curcuits_final_df =add_ingestion_date(curcuits_renamed_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC ###5. Write data to data lake as parquet

# COMMAND ----------

curcuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
