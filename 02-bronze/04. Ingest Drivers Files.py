# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Data Files Into Bronze
# MAGIC - Reading using dataframe reader api
# MAGIC - Apply and enforce schema but maintain the nested structure of json
# MAGIC - Adding metadata columns
# MAGIC - Writing to bronze layer

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

source_file = f"{landing_folder_path}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %run ../00-helper/02.bronze_helper

# COMMAND ----------

# MAGIC %md
# MAGIC Define the schema

# COMMAND ----------

#We define nested schema

from pyspark.sql.types import StructType, StructField, StringType, DateType
name_schema = StructType([
    StructField('givenName',StringType()),
    StructField('familyName',StringType())
])

#As it is a nested JSON, and name field has a different schema, we can pass its schema onto the schema of the drivers JSON

drivers_schema = StructType([
    StructField('driverId',StringType()),
    StructField('name',name_schema),
    StructField('dateOfBirth',DateType()),
    StructField('nationality',StringType()),
    StructField('url',StringType())

])


# COMMAND ----------

drivers_df = (
    spark.read
    .format('json')
    .schema(drivers_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)
display(drivers_df)

# COMMAND ----------

#Adding Metadata Columns 
#metadata function from the bronze helper
drivers_final_df = add_metadata(drivers_df)
    
display(drivers_final_df)

# COMMAND ----------

(
    drivers_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

