# Databricks notebook source
# MAGIC %md
# MAGIC ![image_1778667038005.png](./image_1778667038005.png "image_1778667038005.png")

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"


# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_drivers_df = (
    drivers_df
    .join(
        nationality_region_df,
        drivers_df.nationality == nationality_region_df.nationality,
        "left" #We may have missed some nationalities in the reference table but we need more importantly constructors data
    )
    .select( #Following is all the columns needed
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        nationality_region_df.region.alias("nationality_region")
    )
)

# COMMAND ----------

dim_drivers_df.display()

# COMMAND ----------

( 
 dim_drivers_df
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

