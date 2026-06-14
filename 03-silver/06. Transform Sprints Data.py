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

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"
from pyspark.sql import functions as F



# COMMAND ----------

sprints_df = spark.table(bronze_table)
display(sprints_df)

# COMMAND ----------

sprints_df.columns

# COMMAND ----------

#dropping the url column
sprints_select_df = sprints_df.select(
    'date',
 'raceName',
 'round',
 'season',
 'constructorId',
 'driverId',
 'grid',
 'laps',
 'number',
 'points',
 'position',
 'positionText',
 'status',
 'ingestion_timestamp',
 'source_file'
)

# COMMAND ----------

#Standardizing the column names

sprints_renamed_df = sprints_select_df.withColumnsRenamed({
    "raceName":"race_name",
    "constructorId": "constructor_id",
    "driverId": "driver_id",
    "date":"race_date",
    "grid":"grid_position",
    "laps":"completed_laps",
    "number":"car_number",
    "position":"final_position",
    "positionText":"final_position_text"
})

display(sprints_renamed_df)

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter("circuit_id IS NOT NULL")

sprints_valid_df = sprints_renamed_df.filter(
    F.col("season").isNotNull() & F.col("driver_id").isNotNull() & F.col("round").isNotNull() & 
    F.col("constructor_id").isNotNull()
) #Business Key Values not null

# COMMAND ----------

display(sprints_renamed_df.count()-sprints_valid_df.count())

# COMMAND ----------

#Fixing the duplicate values

# circuits_distinct_df = circuits_valid_df.distinct()

sprints_distinct_df = sprints_valid_df.dropDuplicates(["driver_id","season","constructor_id","round"]) 
display(sprints_distinct_df)

# COMMAND ----------

#Capitalize the circuit name column
from pyspark.sql import functions as F
sprints_final_df = (
    sprints_distinct_df
    .withColumn("race_name",F.initcap(F.col("race_name"))) #Capitalizing the first letter of each word
    )
display(sprints_final_df)

# COMMAND ----------

#Writing to the silver table
(
    sprints_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
 )

# COMMAND ----------

spark.table(silver_table).display()

# COMMAND ----------

