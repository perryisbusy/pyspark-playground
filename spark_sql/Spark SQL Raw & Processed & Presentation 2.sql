-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## `Raw Layer`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `every table is external table`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `create tables csv in database, f1_raw`

-- COMMAND ----------

create database if not exists f1_raw;
use f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.circuits`

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string)
using csv
options (path "dbfs:/mnt/databrickssa1/raw/circuits.csv", header true);

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.races`

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceId int,
year int,
round int,
circuitId int,
name string,
date string,
time string,
url string)
using csv
options (path "dbfs:/mnt/databrickssa1/raw/races.csv", header True);

-- COMMAND ----------

select * from races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.constructors`

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING)
using json
options (path "dbfs:/mnt/databrickssa1/raw/constructors.json");

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.drivers`

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
name struct<forename: string, surname: string>, -- having forename and surname in the name struct
dob date,
nationality string,
url string
)
using json
options (path "dbfs:/mnt/databrickssa1/raw/drivers.json");

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.results`

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int, 
grid int,
position int,
positionText string,
positionOrder int,
points int,
laps int,
time string,
milliseconds string,
fastestLap string,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId int
)
using json
options (path "dbfs:/mnt/databrickssa1/raw/results.json");

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.pit_stops`

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
raceId int,
driverId int,
stop int,
lap int,
time string,
duration string,
milliseconds int
)
using json
options (path "dbfs:/mnt/databrickssa1/raw/pit_stops.json", multiLine True);

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.lap_times`

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
race_id int,
driver_id int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options (path "dbfs:/mnt/databrickssa1/raw/lap_times/", header True)

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_raw.qualifying`

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2 string,
q3 string
)
using json
options (path "dbfs:/mnt/databrickssa1/raw/qualifying/", multiLine True);

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `Processed Layer`

-- COMMAND ----------

create database if not exists f1_processed;
use f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.circuits`

-- COMMAND ----------

drop table if exists f1_processed.circuits;
create table if not exists f1_processed.circuits(
circuit_id int,
circuit_ref string,
name string,
location string,
country string,
latitude double,
longitude double,
altitude int,
ingestion_date timestamp)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/circuits/");

-- COMMAND ----------

select * from f1_processed.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.races`

-- COMMAND ----------

drop table if exists f1_processed.races;
create table if not exists f1_processed.races(
race_id int,
round int,
circuit_id int,
name string,
race_timestamp timestamp,
ingestion_date timestamp,
race_year int
)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/races/");

-- COMMAND ----------

select * from f1_processed.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.constructors`

-- COMMAND ----------

drop table if exists f1_processed.constructors;
create table if not exists f1_processed.constructors(
constructor_id INT, 
constructor_ref STRING, 
name STRING, 
nationality STRING, 
ingestion_date timestamp)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/constructors/");

-- COMMAND ----------

select * from f1_processed.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.drivers`

-- COMMAND ----------

drop table if exists f1_processed.drivers;
create table if not exists f1_processed.drivers(
driver_id int,
driver_ref string,
number int,
code string,
name string, 
dob date,
nationality string,
ingestion_date timestamp
)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/drivers/");

-- COMMAND ----------

select * from f1_processed.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.results`

-- COMMAND ----------

drop table if exists f1_processed.results;
create table if not exists f1_processed.results(
result_id int,
driver_id int,
constructor_id int,
number int, 
grid int,
position int,
position_text string,
position_order int,
points int,
laps int,
time string,
milliseconds string,
fastestLap string,
rank int,
fastest_lap_time string,
fastest_lap_speed float,
ingestion_date timestamp,
race_id int
)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/results/");

-- COMMAND ----------

select * from f1_processed.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.pit_stops`

-- COMMAND ----------

drop table if exists f1_processed.pit_stops;
create table if not exists f1_processed.pit_stops(
race_id int,
driver_id int,
stop int,
lap int,
time string,
duration double,
milliseconds long,
ingestion_date timestamp
)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/pit_stops/");

-- COMMAND ----------

select * from f1_processed.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.lap_times`

-- COMMAND ----------

drop table if exists f1_processed.lap_times;
create table if not exists f1_processed.lap_times(
race_id int,
driver_id int,
lap int,
position int,
time string,
milliseconds int,
ingestion_date timestamp
)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/lap_times/")

-- COMMAND ----------

select * from f1_processed.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_processed.qualifying`

-- COMMAND ----------

drop table if exists f1_processed.qualifying;
create table if not exists f1_processed.qualifying(
qualify_id int,
race_id int,
driver_id int,
constructor_id int,
number int,
position int,
q1 string,
q2 string,
q3 string,
ingestion_date timestamp
)
using parquet
options (path "dbfs:/mnt/databrickssa1/processed/qualifying/");

-- COMMAND ----------

select * from f1_processed.qualifying;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## `Presentation Layer`

-- COMMAND ----------

create database if not exists f1_presentation;
use f1_presentation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_presentation.race_results`

-- COMMAND ----------

drop table if exists f1_presentation.race_results;
create table if not exists f1_presentation.race_results(
race_year int,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
team string,
grid int,
fastest_lap int,
race_time string,
points int,
position int,
ingestion_date timestamp
)
using parquet
options (path "dbfs:/mnt/databrickssa1/presentation/race_results/");

-- COMMAND ----------

select * from f1_presentation.race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_presentation.driver_standings`

-- COMMAND ----------

drop table if exists f1_presentation.driver_standings;
create table if not exists f1_presentation.driver_standings(
race_year int,
driver_name string,
driver_nationality string,
team string,
wins long,
points long,
rank int)
using parquet
options (path  "dbfs:/mnt/databrickssa1/presentation/driver_standings/");

-- COMMAND ----------

select * from driver_standings;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `f1_presentation.constructor_standings`

-- COMMAND ----------

drop table if exists f1_presentation.constructor_standings;
create table if not exists f1_presentation.constructor_standings(
race_year int,
team string,
wins long,
points long,
rank int
)
using parquet
options (path  "dbfs:/mnt/databrickssa1/presentation/constructor_standings/");

-- COMMAND ----------

select * from f1_presentation.constructor_stanings;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

