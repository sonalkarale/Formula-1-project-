-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

 show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select *, driver_ref||' '||code as new_driver_ref from drivers;

-- COMMAND ----------

select *, split(name,' ')[0] forename ,split(name,' ')[1] surname from drivers;

-- COMMAND ----------

select *, current_timestamp()
from drivers;

-- COMMAND ----------

select *, date_format(dob,'dd-MM-yyyy')
 from drivers

-- COMMAND ----------

select *, date_add(dob,1) from 
from drivers;

-- COMMAND ----------

select count(*) from  drivers;

-- COMMAND ----------

select max(dob) from drivers
where dob ='2000-05-11'


-- COMMAND ----------

select count(*) from drivers where nationality = 'British'

-- COMMAND ----------

select nationality, name, count(*)from drivers
where nationality = 'British'
group by nationality, name 
having count(*) = 1
order by nationality;

-- COMMAND ----------

select nationality, count(*) from drivers 
group by nationality 
having count(*) >100 
order by nationality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####window function

-- COMMAND ----------

select nationality, name ,dob,
rank() over(partition by nationality order by dob desc) as age_rank
from drivers 
order by nationality, age_rank

-- COMMAND ----------


