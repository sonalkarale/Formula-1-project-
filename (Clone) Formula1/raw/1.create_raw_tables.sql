-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###create circuits tabels

-- COMMAND ----------

create table if not exists f1_raw.circuits(circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string
)
using csv
options (path "/mnt/formula1datalake24/raw/circuits.csv")

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hive_metastore.formula1datalake24

-- COMMAND ----------

SELECT current_catalog()

-- COMMAND ----------

USE CATALOG hive_metastore;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE SCHEMA formula1datalake24;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string
)
using csv
options (path "/mnt/formula1datalake24/raw/circuits.csv",header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### create races table 

-- COMMAND ----------

drop table  if exists f1_raw.circuits;
create table if not exists f1_raw.races(raceId int,
year int ,
round int,
circuitId int,
name string,
date date,
time string,
url string
)
using csv
options (path "/mnt/formula1datalake24/raw/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create tables for JSON files 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create constructor table 
-- MAGIC 1. simple line JSON
-- MAGIC 2. Simple Structure 

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId int,
constructorRef string,
name string,
nationality string,
url string
)
using json 
options(path "/mnt/formula1datalake24/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create driver table
-- MAGIC 1. Single line JSON 
-- MAGIC 2. Complex Structure
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
name Struct<forename: string , surname: string>,
dob date,
nationality string,
url string
)
using json 
options(path "/mnt/formula1datalake24/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table 
-- MAGIC 1. Single line JSON 
-- MAGIC 2. Simple Structure 

-- COMMAND ----------

drop table if exists f1_raw_results ;
create table if not exists f1_raw.results(
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fasterLap int ,
rank int,
fasterLapTime string,
fasterLapspeed float,
statusId string
)
using json
options (path "/mnt/formula1datalake24/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create Pit Stop tables
-- MAGIC 1. multiline json
-- MAGIC 2. simple Structure 

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
driverId int,
duration string,
lap int,
milliseconds int,
raceId int,
stop int ,
time string
)
using json 
options(path "/mnt/formula1datalake24/raw/pit_stops.json",multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### create table listsof files 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create Lap times table 
-- MAGIC 1. csv file 
-- MAGIC 2. Multiple files 

-- COMMAND ----------

drop table if exists f1_raw.lap_times ;
create table if not exists f1_raw.lap_times(
raceId int ,
driverId int,
lap int,
position int ,
time string,
milliseconds int 
)
using csv
options (path "/mnt/formula1datalake24/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times ;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create qualifying table 
-- MAGIC 1. JSON file 
-- MAGIC 2. MUltiline JSON
-- MAGIC 3. MUltiple files 
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
constructorId int ,
driverId int,
number int,
position int ,
q1 string ,
q2 string ,
q3 string ,
qualifyId int,
raceId int
)
using json 
options(path "/mnt/formula1datalake24/raw/qualifying",multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying

-- COMMAND ----------


