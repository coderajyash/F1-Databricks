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

target_table = f"{catalog_name}.{gold_schema}.dim_races"


# COMMAND ----------

circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catalog_name}.{silver_schema}.races")

# COMMAND ----------

dim_races_df = (
    races_df
    .join(
        circuits_df,
        races_df.circuit_id == circuits_df.circuit_id,
        "inner"
    )
    .select( #Following is all the columns needed
        races_df.season,
        races_df.round,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_name,
        circuits_df.locality,
        circuits_df.country
    )
)

# COMMAND ----------

dim_races_df.display()

# COMMAND ----------

( 
 dim_races_df
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

