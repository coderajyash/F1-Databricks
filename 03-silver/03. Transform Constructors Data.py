# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read bronze layer data
# MAGIC IMP: This dataset containes nested names column, it has to be handled here
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

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"


# COMMAND ----------

# constructors_df = spark.read.table(bronze_table) This is more customizable as it has options.
constructors_df = spark.table(bronze_table) #Works same as above
display(constructors_df)

# COMMAND ----------

constructors_df.columns

# COMMAND ----------

#dropping the url column
constructors_select_df = constructors_df.select(
    'constructorId',
 'name',
 'nationality',
 'ingestion_timestamp',
 'source_file'
)

# COMMAND ----------

#Standardizing the column names

constructors_renamed_df = constructors_select_df.withColumnsRenamed({
    "constructorId": "constructor_id",
    "name": "constructor_name",
})

display(constructors_renamed_df)

# COMMAND ----------

# #Fixing data quality issues
# from pyspark.sql import functions as F

# # circuits_valid_df = circuits_renamed_df.filter("circuit_id IS NOT NULL")
# #Same results
# constructors_valid_df = constructors_renamed_df.filter(
#     F.col("circuit_id").isNotNull()
# )


# COMMAND ----------

#Fixing the duplicate values

# circuits_distinct_df = circuits_valid_df.distinct()

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"]) 
display(constructors_distinct_df)

# COMMAND ----------

#Capitalize the circuit name column
from pyspark.sql import functions as F
constructors_final_df = (
    constructors_distinct_df
    .withColumn("nationality",F.initcap(F.col("nationality"))) #Capitalizing the first letter of each word
    )
display(constructors_final_df)

# COMMAND ----------

#Writing to the silver table
(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
 )

# COMMAND ----------

spark.table(silver_table).display()

# COMMAND ----------

