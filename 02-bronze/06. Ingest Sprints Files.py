# Databricks notebook source
# MAGIC %md
# MAGIC Ingesting multiple line json files data from a folder.

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

source_file = f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %run ../00-helper/02.bronze_helper

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

sprints_schema = StructType([
    StructField('date',            DateType()),
    StructField('raceName',        StringType()),
    StructField('round',           IntegerType()),
    StructField('season',          IntegerType()),
    StructField('url',             StringType()),
    StructField('constructorId',   StringType()),
    StructField('driverId',        StringType()),
    StructField('grid',            IntegerType()),
    StructField('laps',            IntegerType()),
    StructField('number',          IntegerType()),
    StructField('points',          FloatType()),
    StructField('position',        IntegerType()),
    StructField('positionText',    StringType()),
    StructField('status',          StringType())
])

# COMMAND ----------

sprints_df = (
    spark.read
    .format('json')
    .schema(sprints_schema)
    .option('mode','FAILFAST')
    .option('multiline',True) #Change for multiline json
    .load(source_file)
)
display(sprints_df)

# COMMAND ----------

sprints_final_df = add_metadata(sprints_df)
    
display(sprints_final_df)

# COMMAND ----------

(
    sprints_final_df
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
# MAGIC FROM formula1.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

