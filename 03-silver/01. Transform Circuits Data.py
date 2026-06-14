# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read bronze layer data
# MAGIC 2. Drop unnecessary columns
# MAGIC 3. Standardize column names
# MAGIC 4. Rename column names for meaningful insights
# MAGIC 5. Filter out rows with null circuit id.(No use, Business Key Validation)
# MAGIC 6. Handle duplicate records
# MAGIC 7. Transform circuit name and locality to title case for better reporting
# MAGIC 8. Write the transformed data to silver layer

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"


# COMMAND ----------

# races_df = spark.read.table(bronze_table) This is more customizable as it has options.
circuits_df = spark.table(bronze_table) #Works same as above
display(circuits_df)

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

#dropping the url column
circuits_select_df = circuits_df.select(
    'circuitId',
 'circuitName',
 'lat',
 'long',
 'locality',
 'country',
 'ingestion_timestamp',
 'source_file'
)

# COMMAND ----------

#Standardizing the column names

# circuits_renamed_df = circuits_select_df.withColumnRenamed('circuitId','circuit_id') \
#     .withColumnRenamed("circuitName","circuit_name") \
#     .withColumnRenamed("lat","latitude") \
#     .withColumnRenamed("long","longitude") \

circuits_renamed_df = circuits_select_df.withColumnsRenamed({
    "circuitId": "circuit_id",
    "circuitName": "circuit_name",
    "lat": "latitude",
    "long": "longitude"
})

display(circuits_renamed_df)

# COMMAND ----------

#Fixing data quality issues
from pyspark.sql import functions as F

# circuits_valid_df = circuits_renamed_df.filter("circuit_id IS NOT NULL")
#Same results
circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)


# COMMAND ----------

#Fixing the duplicate values

# circuits_distinct_df = circuits_valid_df.distinct()

circuits_distinct_df = circuits_valid_df.dropDuplicates()

display(circuits_distinct_df)

# COMMAND ----------

#Capitalize the circuit name column

circuits_final_df = (
    circuits_distinct_df
    .withColumn("circuit_name",F.initcap(F.col("circuit_name"))) #Capitalizing the first letter of each word
    .withColumn("locality",F.initcap(F.col("locality")))
    )
display(circuits_final_df)

# COMMAND ----------

#Writing to the silver table
(
    circuits_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
 )

# COMMAND ----------

spark.table(silver_table).display()

# COMMAND ----------

