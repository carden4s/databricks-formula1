# Databricks notebook source
# Importo las funciones de Spark SQL con el alias f
from pyspark.sql import functions as f

# Agrego columnas de auditoría: fecha/hora de ingesta y archivo de origen
def add_ingestion_metadata(df):
    return( df.withColumn('ingestion_timestamps', f.current_timestamp())
        .withColumn('source_file', f.col('_metadata.file_path'))
    )
