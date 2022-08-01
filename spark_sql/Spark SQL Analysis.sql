-- Databricks notebook source
use f1_processed;
select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `create a managed table`

-- COMMAND ----------

-- %python
-- dbutils.fs.rm("dbfs:/user/hive/warehouse/f1_presentation.db/calculated_race_results/", True)

-- COMMAND ----------

drop table if exists f1_presentation.calculated_race_results;

create table if not exists f1_presentation.calculated_race_results
using parquet
select races.race_year,
constructors.name as constructor_name,
drivers.name as driver_name,
results.position,
results.points,
11 - results.position as calculated_points
from f1_processed.results 
inner join f1_processed.drivers on results.driver_id = drivers.driver_id
inner join f1_processed.constructors on results.constructor_id = constructors.constructor_id
inner join f1_processed.races on results.race_id = races.race_id
where results.position <= 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `dominant drivers`

-- COMMAND ----------

select driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
dense_rank() over (order by avg(calculated_points) desc) as drank
from f1_presentation.calculated_race_results
where race_year between 2001 and 2020
group by driver_name
having total_races >= 50
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `dominant constructors`

-- COMMAND ----------

select constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
dense_rank() over (order by avg(calculated_points) desc) as drank
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by constructor_name
having total_races >= 100
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `dominant drivers viz`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `dominant constrcutors/teams viz`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_constructor
AS
SELECT constructor_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
  FROM f1_presentation.calculated_race_results
GROUP BY constructor_name
HAVING total_races >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT race_year, 
       constructor_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE constructor_name IN (SELECT constructor_name FROM v_dominant_constructor WHERE team_rank <= 5)
GROUP BY race_year, constructor_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

