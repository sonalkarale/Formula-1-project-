# Databricks notebook source
# MAGIC %md
# MAGIC #### produce Driver standing 

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### find the race years for which the data is to be  reprocessed 

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
                        .filter(f"file_date = '{v_file_date}'")
                        

# COMMAND ----------

# race_results_list

# COMMAND ----------

# race_year_list = []
# for race_year in race_results_list:
#     race_year_list.append(race_year.race_year)



# COMMAND ----------

race_year_list = df_column_to_list(race_results_list,'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import  sum , count,col,when
driver_standing_df = race_results_df\
                    .groupBy("race_year","driver_name","driver_nationality")\
                    .agg(sum("points").alias("total_points"),
                         count(when(col("position")== 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.functions import col,sum ,when ,count, desc,rank
from pyspark.sql.window import Window

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standing")

# COMMAND ----------

# overwrite_partition(final_df,"f1_presentation",'driver_standings','race_year')

# COMMAND ----------

# race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings
# MAGIC where race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year, count(1) from f1_presentation.race_results
# MAGIC group by race_year
# MAGIC order by race_year desc

# COMMAND ----------


