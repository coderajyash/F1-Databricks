# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read bronze layer data
# MAGIC IMP: This dataset containes nested names column, it has to be handled here
# MAGIC
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

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"


# COMMAND ----------

drivers_df = spark.table(bronze_table) #Works same as above
display(drivers_df)

# COMMAND ----------

drivers_df.columns

# COMMAND ----------

#dropping the url column
drivers_select_df = drivers_df.select(
    'driverId',
 'name',
 'dateOfBirth',
 'nationality',
 'ingestion_timestamp',
 'source_file'
)

# COMMAND ----------

#Standardizing the column names

drivers_renamed_df = drivers_select_df.withColumnsRenamed({
    "name":"driver_name",
    "driverId": "driver_id",
    "dateOfBirth": "date_of_birth",
})

display(drivers_renamed_df)

# COMMAND ----------

#Combine the given name and family name into one column, i.e. driver_name
from pyspark.sql import functions as F

drivers_renamed_df = (
    drivers_renamed_df
    .withColumn("driver_name", F.concat(F.col("driver_name.givenName"), F.lit(" "), F.col("driver_name.familyName")))
)
display(drivers_renamed_df)


# COMMAND ----------

#Fixing the duplicate values

# circuits_distinct_df = circuits_valid_df.distinct()

drivers_distinct_df = drivers_renamed_df.dropDuplicates(["driver_id"]) 
display(drivers_distinct_df)

# COMMAND ----------

#Capitalize the circuit name column
from pyspark.sql import functions as F
drivers_final_df = (
    drivers_distinct_df
    .withColumn("nationality",F.initcap(F.col("nationality"))) 
    .withColumn("driver_name",F.initcap(F.col("driver_name"))) #Capitalizing the first letter of each word

    )
display(drivers_final_df)

# COMMAND ----------

#Writing to the silver table
(
    drivers_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
 )

# COMMAND ----------

spark.table(silver_table).display()

# COMMAND ----------

