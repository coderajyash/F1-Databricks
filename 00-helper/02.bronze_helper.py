# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

def add_metadata(df):
    return (
    df
    .withColumn('ingestion_timestamp', current_timestamp())
    .withColumn('source_file', col('_metadata.file_path'))
    )