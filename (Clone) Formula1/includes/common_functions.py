# Databricks notebook source
raw_folder_path = '/mnt/formula1datalake24/raw'
processed_folder_path = '/mnt/formula1datalake24/processed'
presentation_folder_path ='/mnt/formula1datalake24/presentation'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    # Rearrange the partition column
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    input_df = input_df.select(column_list)
    
    # Set partition overwrite mode
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Check if the table exists
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        # If table exists, overwrite it
        input_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        # If table doesn't exist, create it with partitioning
        input_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df,column_name):
    df_row_list = input_df.select(column_name)\
        .distinct()\
        .collect()

    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def merge_delta_data(input_df,db_name,table_name,folder_path,merge_condition,partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark,f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------


