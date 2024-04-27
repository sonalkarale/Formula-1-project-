-- Databricks notebook source
select driver_name,count(1) as total_races, avg(calculated_points) as avg_points,
 sum(calculated_points) as total_points from f1_presentation.calculated_race_results
group by driver_name
having total_races >=50 
order by avg_points desc;

-- COMMAND ----------

select driver_name,count(1) as total_races, avg(calculated_points) as avg_points,
 sum(calculated_points) as total_points from f1_presentation.calculated_race_results
 where race_year between 2011 and 2020
group by driver_name
having total_races >50 
order by avg_points desc;

-- COMMAND ----------


