# Databricks notebook source
# MAGIC %md
# MAGIC Goal is to generate dim_races table from races and circuits table

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1778667038005.png](./image_1778667038005.png "image_1778667038005.png")

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"


# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(
        nationality_region_df,
        constructors_df.nationality == nationality_region_df.nationality,
        "left" #We may have missed some nationalities in the reference table but we need more importantly constructors data
    )
    .select( #Following is all the columns needed
        constructors_df.constructor_id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        nationality_region_df.region.alias("nationality_region")
    )
)

# COMMAND ----------

dim_constructors_df.display()

# COMMAND ----------

( 
 dim_constructors_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(target_table)
)


# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

