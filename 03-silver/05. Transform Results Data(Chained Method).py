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

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"
from pyspark.sql import functions as F

# COMMAND ----------

results_df = (
    spark
    .table(bronze_table)
    .select(
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
    .withColumnsRenamed({
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
    .filter(
        F.col("season").isNotNull() & F.col("driver_id").isNotNull() & F.col("round").isNotNull() & 
        F.col("constructor_id").isNotNull()
    )
    .dropDuplicates(
        ["driver_id","season","constructor_id","round"]
    )
    .withColumn(
        "race_name",F.initcap(F.col("race_name"))
    )
)

# COMMAND ----------

#Writing to the silver table
(
    results_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
 )

# COMMAND ----------

spark.table(silver_table).display()

# COMMAND ----------

