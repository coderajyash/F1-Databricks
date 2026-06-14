# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Data Files Into Bronze
# MAGIC - Reading using dataframe reader api
# MAGIC - Apply DDL schema onto this (Can also be done using StructType)
# MAGIC - Adding metadata columns
# MAGIC - Writing to bronze layer

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %run ../00-helper/02.bronze_helper

# COMMAND ----------

#Defining the schema

constructors_schema = "constructorId STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructors_df = (
    spark.read
    .format('json')
    .schema(constructors_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)
display(constructors_df)


# COMMAND ----------

#Adding Metadata Columns 
#metadata function from the bronze helper
constructors_final_df = add_metadata(constructors_df)
    
display(constructors_final_df)

# COMMAND ----------

(
    constructors_final_df
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



# COMMAND ----------

