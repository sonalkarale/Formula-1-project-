-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from f1_processed.drivers;

-- COMMAND ----------

desc drivers;

-- COMMAND ----------

select * from f1_processed.drivers
limit 10

-- COMMAND ----------

select name,dob as date_of_birth from drivers
where nationality = 'British'
and dob >= '1990-01-01'
order by dob desc;

-- COMMAND ----------

select * from drivers
order by nationality asc , dob desc;

-- COMMAND ----------

select name , nationality, dob from drivers
where (nationality = 'British'
  and dob >='1990-01-01')
  or nationality = 'Indian'
order by dob desc;

-- COMMAND ----------


