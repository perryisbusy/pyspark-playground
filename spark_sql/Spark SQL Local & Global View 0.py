# Databricks notebook source
# MAGIC %md
# MAGIC #### `Access dataframes using SQL`
# MAGIC ##### `1, Local Temp View`
# MAGIC ##### `2, Global Temp View`

# COMMAND ----------

# MAGIC %md
# MAGIC ###### `configure the file`

# COMMAND ----------

# MAGIC %run "playground/includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results/")
race_results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `createTempView()` 
# MAGIC ##### `createOrReplaceTempView()`
# MAGIC only available in the spark session and current notebook

# COMMAND ----------

# race_results_df.createTempView("v_race_results")

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### `sql`

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from v_race_results
# MAGIC where race_year = 2020;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `spark.sql()`

# COMMAND ----------

race_results_2020_df = spark.sql("select * from v_race_results where race_year = 2020")
race_results_2020_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `createGlobalTemplView()` 
# MAGIC ##### `createOrReplaceGlobalTempView()`
# MAGIC Available in all notebooks in the current cluster

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results/")
race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- show tables;
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from global_temp.gv_race_results
# MAGIC where race_year = 2020;

# COMMAND ----------

race_results_2020_df = spark.sql("select * from global_temp.gv_race_results where race_year = 2020")
race_results_2020_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

