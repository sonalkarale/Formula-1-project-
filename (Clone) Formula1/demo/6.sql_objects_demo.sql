-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Create Database Demo 
-- MAGIC 3. Data tab in UI
-- MAGIC 4. SHow Command
-- MAGIC 5. Describe Command
-- MAGIC 6. Find the current database

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo ;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

 select current_database();

-- COMMAND ----------

show tables in demo ;

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables in default ;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Learning objective 
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. create Mannaged table using SQL
-- MAGIC 3. effect of dropping a managed table 
-- MAGIC 4. Describe table 
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("Delta").saveAsTable("demo.race_results_python")
-- MAGIC

-- COMMAND ----------

use demo ;
show tables ;

-- COMMAND ----------

describe race_results_python;

-- COMMAND ----------

describe extended race_results_python;

-- COMMAND ----------

select * from demo.race_results_python;

-- COMMAND ----------

select * from demo.race_results_python 
where race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create table if not exists demo.race_result_sql
as 
select * from demo.race_results_python 
where race_year = 2020;

-- COMMAND ----------

select * from  demo.race_result_sql


-- COMMAND ----------

describe extended demo.race_result_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives 
-- MAGIC 1. Create external table using Python 
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table 
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### View on Tables 
-- MAGIC 1. Create temp view
-- MAGIC 2. Crete Global temp View
-- MAGIC 3. Create permanant view

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create or replace temp view v_race_results
as 
select * from demo.race_results_python
where race_year = 2018;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * from demo.race_results_python
where race_year = 2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp;


-- COMMAND ----------


