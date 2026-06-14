# Databricks notebook source
# MAGIC %md
# MAGIC Ingesting multiple json files data from a folder.

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

source_file = f"{landing_folder_path}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %run ../00-helper/02.bronze_helper

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

results_schema = StructType([
    StructField('date',DateType()),
    StructField('raceName',StringType()),
    StructField('round',IntegerType()),
    StructField('season',IntegerType()),
    StructField('url',StringType()),
    StructField('constructorId',StringType()),
    StructField('driverId',StringType()),
    StructField('grid',IntegerType()),
    StructField('laps',IntegerType()),
    StructField('number',IntegerType()),
    StructField('points',FloatType()),
    StructField('position',IntegerType()),
    StructField('positionText',StringType()),
    StructField('status',StringType())
])

# COMMAND ----------

results_df = (
    spark.read
    .format('json')
    .schema(results_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)
display(results_df)

# COMMAND ----------

results_final_df = add_metadata(results_df)
    
display(results_final_df)

# COMMAND ----------

(
    results_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, COUNT(*)
# MAGIC FROM formula1.bronze.results
# MAGIC GROUP BY season
# MAGIC ORDER BY season

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

