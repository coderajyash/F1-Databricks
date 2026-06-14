# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Data Files Into Bronze
# MAGIC - Reading using dataframe reader api
# MAGIC - Adding metadata columns
# MAGIC - Writing to bronze layer

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
races_schema = StructType([
                    StructField('season', IntegerType()),
                    StructField('round', IntegerType()),
                    StructField('url', StringType()),
                    StructField('raceName', StringType()),
                    StructField('date', DateType()),
                    StructField('circuitId', StringType())
                ])

# COMMAND ----------

races_df = (
        spark.read
            .format('csv')
            .option('header', True)
            .option('mode','FAILFAST')
            .schema(races_schema)
            .load(source_file)
            )

# COMMAND ----------

display(races_df)

# COMMAND ----------

#Adding Metadata Columns 

from pyspark.sql.functions import current_timestamp, col
races_final_df = (
    races_df
    .withColumn('ingestion_timestamp', current_timestamp())
    .withColumn('source_file', col('_metadata.file_path'))
    )
display(races_final_df)


# COMMAND ----------

(
    races_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------



# COMMAND ----------

