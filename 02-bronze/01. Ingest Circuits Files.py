# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Data Files Into Bronze
# MAGIC - Reading using dataframe reader api
# MAGIC - Adding metadata columns
# MAGIC - Writing to bronze layer

# COMMAND ----------

circuits_df = (
        spark.read
            .format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .load('/Volumes/formula1/landing/files/circuits.csv')
            )

# COMMAND ----------

circuits_df.show(10)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC inferSchema : This allows spark to analyse the data and infer the schema, i.e. the data types of each column(if not then default is text). This is not recommended for production datasets as the data can change and the schema will also, but we need our data to be predictable.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuits_schema = StructType([
                    StructField('circuitId', StringType()),
                    StructField('url', StringType()),
                    StructField('circuitName', StringType()),
                    StructField('lat', DoubleType()),
                    StructField('long', DoubleType()),
                    StructField('locality', StringType()),
                    StructField('country', StringType())
                ])

# COMMAND ----------

circuits_df = (
        spark.read
            .format('csv')
            .option('header', True)
            .schema(circuits_schema)
            .load('/Volumes/formula1/landing/files/circuits.csv')
            )

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

#Adding Metadata Columns 

from pyspark.sql.functions import current_timestamp, col
circuits_final_df = (
    circuits_df
    .withColumn('ingestion_timestamp', current_timestamp())
    .withColumn('source_file', col('_metadata.file_path'))
    )
display(circuits_final_df)


# COMMAND ----------

(
    circuits_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('formula1.bronze.circuits')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Better Managable Codeblock

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

#Now we can access environment object names
catalog_name

# COMMAND ----------

# MAGIC %md
# MAGIC Source File Name and Table Name can change project to project so instead of keeping them hardcoded, we make it more managable

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuits_schema = StructType([
                    StructField('circuitId', StringType()),
                    StructField('url', StringType()),
                    StructField('circuitName', StringType()),
                    StructField('lat', DoubleType()),
                    StructField('long', DoubleType()),
                    StructField('locality', StringType()),
                    StructField('country', StringType())
                ])

# COMMAND ----------

circuits_df = (
        spark.read
            .format('csv')
            .option('header', True)
            .schema(circuits_schema)
            .load(source_file)
            )

# COMMAND ----------

# MAGIC %md
# MAGIC Adding metadata is also a repeated task so can be pushed into a helper function

# COMMAND ----------

# MAGIC %run ../00-helper/02.bronze_helper

# COMMAND ----------

#Adding Metadata Columns 
#metadata function from the bronze helper
circuits_final_df = add_metadata(circuits_df)
    
display(circuits_final_df)

# COMMAND ----------

(
    circuits_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

