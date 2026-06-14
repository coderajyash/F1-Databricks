# Databricks notebook source
# MAGIC %md
# MAGIC - Race and Sprint are the session type(This should be mapped accordingly in the fact table)
# MAGIC - The other extra attributes are derived from the present attributes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1778667038005.png](./image_1778667038005.png "image_1778667038005.png")

# COMMAND ----------

# MAGIC %run ../00-helper/01.env_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.results_sprints"


# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .withColumn('session_type',F.lit("RACE")) #Adds literal value of "Race" to every column
        .drop("race_date","race_name","ingestion_timestamp","source_file")
    )

# COMMAND ----------

display(results_df)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
        .withColumn('session_type',F.lit("SPRINTS")) #Adds literal value of "Race" to every column
        .drop("race_date","race_name","ingestion_timestamp","source_file")
    )

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)
display(results_sprints_df)

# COMMAND ----------

#Adding Derived Columns
fact_session_results_df =(
    results_sprints_df
        .withColumn("is_win",F.col("final_position") == 1)
        .withColumn("is_podium",F.col("final_position").between(1,3))
        .withColumn("has_points",F.col("points") > 0)
        
)

# COMMAND ----------

display(fact_session_results_df.filter("season = 2025"))

# COMMAND ----------

( 
 fact_session_results_df
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

