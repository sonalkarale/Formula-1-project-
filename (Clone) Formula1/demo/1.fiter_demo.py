# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df1 = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

display(races_filtered_df1)

# COMMAND ----------

races_filtered_df = races_df.where((races_df["race_year"] == 2019) & (races_df["round"] <=5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


