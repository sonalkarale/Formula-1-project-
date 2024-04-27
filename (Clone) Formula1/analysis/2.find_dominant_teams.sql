-- Databricks notebook source
select team_name,count(1) as total_races, avg(calculated_points) as avg_points,
 sum(calculated_points) as total_points from f1_presentation.calculated_race_results
 where race_year between 2011 and 2020
group by team_name
having total_races >=100 
order by avg_points desc;

-- COMMAND ----------

select team_name,count(1) as total_races, avg(calculated_points) as avg_points,
 sum(calculated_points) as total_points from f1_presentation.calculated_race_results
 where race_year between 2001 and 2011
group by team_name
having total_races >=100 
order by avg_points desc;

-- COMMAND ----------


