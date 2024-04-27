# Databricks notebook source
# MAGIC %md
# MAGIC #### produce constructor standing 

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC find the years for which the data is to be processed 

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,'race_year')

# COMMAND ----------

from pyspark.sql.functions import col,when ,count,sum

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standing_df = race_results_df\
                        .groupBy(col("race_year"),col("team"))\
                        .agg(sum("points").alias("total_points"),count(when(col("position")== 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standing_df.withColumn("rank",rank().over(constructor_rank_spec))  

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standing', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation','constructor_standing','race_year')

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standing")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standing;

# COMMAND ----------


