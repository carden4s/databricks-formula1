# Databricks notebook source
# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------

# MAGIC
# MAGIC %run ../01-setup/01.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"


# COMMAND ----------

constructors_schema = 'constructorId STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = (spark.read
  .format("json")
  .schema(constructors_schema)
  .option('mode', 'FAILFAST')
  .load(source_file)
)
display(constructors_df)



# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)


# COMMAND ----------

(
    constructors_final_df.write 
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))
