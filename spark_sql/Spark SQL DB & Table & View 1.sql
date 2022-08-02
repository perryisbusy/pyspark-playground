-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### `Create Managed Tables using Python`

-- COMMAND ----------

-- MAGIC %run "/playground/includes/configuration/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC race_results_df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### `df.write.saveAsTable()`

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # race_results_df.write.format("parquet").saveAsTable("demo.race_results_man_py")
-- MAGIC 
-- MAGIC # no path
-- MAGIC race_results_df.write.saveAsTable(name="demo.race_results_man_py", format="parquet", mode="overwrite")

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show databases;

-- COMMAND ----------

use demo;
show tables in demo;

-- COMMAND ----------

describe extended race_results_man_py;

-- COMMAND ----------

select *
from race_results_man_py
where lower(driver_name) like "%pi%"
and race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `Create Managed Tables using SQL, which you need to use saveAsTable() before`

-- COMMAND ----------

create table if not exists race_results_man_sql
as
select *
from race_results_man_py;

-- COMMAND ----------

desc extended race_results_man_sql;

-- COMMAND ----------

select *
from race_results_man_sql
where lower(driver_name) like "%pi%"
and race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `Create Unmanaged/External Tables using Python`

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # have path
-- MAGIC race_results_df.write.saveAsTable(name="race_results_ext_py", format="parquet", mode="overwrite", 
-- MAGIC                                   path=f"{presentation_folder_path}/race_results_ext_py/")

-- COMMAND ----------

describe extended demo.race_results_ext_py;

-- COMMAND ----------

select *
from race_results_ext_py;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `Create Unmanaged/External Tables using SQL`

-- COMMAND ----------

create table if not exists race_results_ext_sql(
race_year int,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
team string,
grid int,
fastestlap int,
race_time string,
points float,
position int,
created_date timestamp
)
using parquet
-- you are using sql in the notebook
location "dbfs:/mnt/databrickssa1/presentation/race_results_ext_sql/" 

-- COMMAND ----------

insert into race_results_ext_sql
select * from race_results_ext_py

-- COMMAND ----------

select * from race_results_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `temp view & global temp view & view (permanent)`

-- COMMAND ----------

create or replace temp view v_race_results 
as
select * 
from race_results_man_py;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results 
as
select * 
from race_results_man_py;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

-- global_temp
select * from global_temp.gv_race_results;

-- COMMAND ----------

-- permanent view
create or replace view pv_race_results 
as
select * 
from race_results_man_py;

-- COMMAND ----------

select * from pv_race_results

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

