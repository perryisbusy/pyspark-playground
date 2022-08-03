-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### `DDL & DML`

-- COMMAND ----------

show databases;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe extended f1_processed.drivers;

-- COMMAND ----------

select name, nationality as country, dob as date_of_birth
from f1_processed.drivers 
where (nationality = "British" and dob >= "1990-01-01") or nationality = "Indian"
order by 2 desc
limit 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC select name, nationality as country, dob as date_of_birth
-- MAGIC from f1_processed.drivers 
-- MAGIC where (nationality = "British" and dob >= "1990-01-01") or nationality = "Indian"
-- MAGIC order by 2 desc
-- MAGIC limit 10
-- MAGIC """)\
-- MAGIC .display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `Simple Functions`

-- COMMAND ----------

select 
concat(driver_ref, "-", code) as new_driver_ref1,
driver_ref || "-" || code as new_driver_ref2,
split(name, " ")[0] as first_name,
split(name, " ", 2)[1] as last_name,
current_timestamp,
date_format(dob, "MM-dd-yyyy") as date_format,
date_add(current_timestamp, 100) as date_add
from f1_processed.drivers;

-- COMMAND ----------

select nationality, 
count(driver_id) as cnt ,
dense_rank() over (order by count(driver_id) desc) as drank
from f1_processed.drivers
group by nationality
having cnt >= 50
order by 2 desc, 1 asc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `joins`

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

desc extended driver_standings;

-- COMMAND ----------

-- create views for join demo
create or replace temp view v_driver_stanings_2018
as
select race_year, driver_name, team, points, wins, rank
from f1_presentation.driver_standings
where race_year = 2018;

create or replace temp view v_driver_stanings_2020
as
select race_year, driver_name, team, points, wins, rank
from f1_presentation.driver_standings
where race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `inner join & left outer join & right outer join & full outer join & left semi join & left anti join`

-- COMMAND ----------

select *
from v_driver_stanings_2018 as d_2018
inner join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_stanings_2018 as d_2018
left outer join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_stanings_2018 as d_2018
right outer join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_stanings_2018 as d_2018
full outer join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- only left table 
select *
from v_driver_stanings_2018 as d_2018
left semi join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- select d_2018.*
-- from v_driver_stanings_2018 as d_2018
-- inner join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_stanings_2018 as d_2018
left anti join v_driver_stanings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- 20 * 24 = 480
select *
from v_driver_stanings_2018 as d_2018
cross join v_driver_stanings_2020 d_2020;

-- COMMAND ----------

