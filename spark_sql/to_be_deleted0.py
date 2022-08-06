# Databricks notebook source
# MAGIC %run "playground/includes/configuration/"

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists db cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists db
# MAGIC location "dbfs:/mnt/databrickssa1/deltalake/db"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/databrickssa1/raw/2021-04-18/drivers.json

# COMMAND ----------

drv_df = spark.read.format("json").load("dbfs:/mnt/databrickssa1/raw/2021-04-18/drivers.json")
drv_df.display()

# COMMAND ----------

drv_df.write.saveAsTable(name="db.drv_ext_py", format="parquet", mode="overwrite", partitionBy=None,
                         path="dbfs:/mnt/databrickssa1/deltalake/db/drv_ext_py")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/databrickssa1/deltalake

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in db

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended db.drv_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists db.drv_ext_sql
# MAGIC location "dbfs:/mnt/databrickssa1/deltalake/db/drv_ext_sql"
# MAGIC as
# MAGIC select *
# MAGIC from db.drv_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended db.drv_ext_sql

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists dd

# COMMAND ----------

drv_df.write.saveAsTable(name="dd.drv_man_py", format="orc", mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in dd

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dd.drv_man_py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dd.drv_man_sql
# MAGIC as
# MAGIC select *
# MAGIC from dd.drv_man_py

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dd.drv_man_sql

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists bb

# COMMAND ----------

drv_df.write.saveAsTable(name="bb.drv_ext_py", format="avro", mode="overwrite",
                         path="dbfs:/mnt/databrickssa1/deltalake/bb/drv_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended bb.drv_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bb.drv_ext_sql
# MAGIC using avro
# MAGIC options (path="dbfs:/mnt/databrickssa1/deltalake/bb/drv_ext_sql")
# MAGIC as
# MAGIC select *
# MAGIC from bb.drv_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended bb.drv_ext_sql

# COMMAND ----------

df = spark.sql("select * from bb.drv_ext_sql")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database dd cascade;
# MAGIC drop database bb cascade;
# MAGIC drop database db cascade;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

