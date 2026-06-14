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

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"


# COMMAND ----------

# races_df = spark.read.table(bronze_table) This is more customizable as it has options.
races_df = spark.table(bronze_table) #Works same as above
display(races_df)

# COMMAND ----------

races_df.columns

# COMMAND ----------

#dropping the url column
races_select_df = races_df.select(
    'season',
 'round',
 'raceName',
 'date',
 'circuitId',
 'ingestion_timestamp',
 'source_file'
)

# COMMAND ----------

#Standardizing the column names

races_renamed_df = races_select_df.withColumnsRenamed({
    "circuitId": "circuit_id",
    "raceName": "race_name",
    "date":"race_date"
})

display(races_renamed_df)

# COMMAND ----------

# #Fixing data quality issues
# from pyspark.sql import functions as F

# # circuits_valid_df = circuits_renamed_df.filter("circuit_id IS NOT NULL")
# #Same results
# races_valid_df = races_renamed_df.filter(
#     F.col("circuit_id").isNotNull()
# )


# COMMAND ----------

#Fixing the duplicate values

# circuits_distinct_df = circuits_valid_df.distinct()

races_distinct_df = races_renamed_df.dropDuplicates(["season", "round"]) #season and round act as a primary key, so dropping duplicates based off that

display(races_distinct_df)

# COMMAND ----------

#Capitalize the circuit name column
from pyspark.sql import functions as F
races_final_df = (
    races_distinct_df
    .withColumn("race_name",F.initcap(F.col("race_name"))) #Capitalizing the first letter of each word
    )
display(races_final_df)

# COMMAND ----------

#Writing to the silver table
(
    races_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
 )

# COMMAND ----------

spark.table(silver_table).display()

# COMMAND ----------

