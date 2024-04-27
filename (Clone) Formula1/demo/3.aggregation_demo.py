# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Aggregate Function Demo

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### built-in aggregate function

# COMMAND ----------

races_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(races_result_df)

# COMMAND ----------

demo_df = races_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum,col

# COMMAND ----------

demo_df.select("race_name").count()

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name ='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

# another method 

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name"))\
                                                .withColumnRenamed("points","total_points")\
                                                .withColumnRenamed("race_name", "number_of_races").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### groupBy

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total_point")).show()

# COMMAND ----------

# another way to write 
demo_df.groupBy("driver_name")\
    .sum("points")\
    .show()

# COMMAND ----------

# we can add many aggregate function with the help of agg method

demo_df.groupBy("driver_name")\
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("no_of_races")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Window Function

# COMMAND ----------

demo_df = races_result_df.filter("race_year in (2029,2020)")

# COMMAND ----------

display(races_result_df)

# COMMAND ----------

demo_group_df =demo_df.groupBy("race_year","driver_name")\
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of races")).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))



# COMMAND ----------

demo_group_df.withColumn("rank",rank().over(driver_rank_spec)).show()

# COMMAND ----------


