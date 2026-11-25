# Databricks notebook source
# DBTITLE 1,Ingesting Sprint Results
# MAGIC %md
# MAGIC # Ingesting Sprint Results
# MAGIC Carga los archivos `sprints_YYYY.json` (2021-2025) de landing a la tabla `formula1.bronze.sprints`.
# MAGIC
# MAGIC **Diferencia con results:** estos JSON son **multilínea**. En results cada línea era una fila (`{...}` `{...}`). Aquí cada archivo es una sola lista grande `[ {...}, {...} ]` repartida en muchas líneas. Por eso hay que avisarle a Spark con `multiLine`.

# COMMAND ----------

# DBTITLE 1,Cargar configuracion
# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------

# DBTITLE 1,Cargar helpers
# MAGIC %run ../01-setup/01.bronze-helpers

# COMMAND ----------

# DBTITLE 1,Paso 1: Origen y destino
# MAGIC %md
# MAGIC ### Paso 0 (las dos celdas de arriba): traer la configuración
# MAGIC `%run` ejecuta otro notebook dentro de este, como si copiaras su código aquí. Así tenemos `catalog_name`, `bronze_schema`, `landing_folder_path` y la función `add_ingestion_metadata`.
# MAGIC
# MAGIC ### Paso 1: ¿De dónde leo y adónde escribo?
# MAGIC * `source_file`: la **carpeta** `sprints` con los 5 archivos. Spark los lee todos juntos.
# MAGIC * `table_name`: `formula1.bronze.sprints` (catálogo.esquema.tabla, como `database.schema.table` en Snowflake).

# COMMAND ----------

# DBTITLE 1,Origen y destino
source_file = f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# DBTITLE 1,Paso 2: Esquema
# MAGIC %md
# MAGIC ### Paso 2: Definir el esquema
# MAGIC Los sprints traen **las mismas 14 columnas que results** (cada fila es un piloto en una carrera sprint), así que el esquema es igual. Es como el `CREATE TABLE (...)` de Snowflake: le decimos a Spark qué columnas esperamos y de qué tipo, para que no tenga que adivinar.

# COMMAND ----------

# DBTITLE 1,Esquema de sprints
sprints_schema = """
    season INT,
    round INT,
    raceName STRING,
    date DATE,
    url STRING,
    driverId STRING,
    constructorId STRING,
    number INT,
    grid INT,
    position INT,
    positionText STRING,
    points DOUBLE,
    laps INT,
    status STRING
"""

# COMMAND ----------

# DBTITLE 1,Paso 3: Leer
# MAGIC %md
# MAGIC ### Paso 3: Leer los JSON multilínea
# MAGIC Es igual que en results, con una opción nueva:
# MAGIC * `.option('multiLine', True)`: le dice a Spark "cada archivo es **un solo JSON** grande que ocupa muchas líneas". Si no lo pones, Spark intenta leer cada línea por separado (`[`, `{`, `"date": ...`), no entiende ninguna y con `FAILFAST` el notebook falla. Sin `FAILFAST`, verías una sola columna `_corrupt_record` llena de basura.
# MAGIC * `.option('mode', 'FAILFAST')`: si algo no encaja con el esquema, se detiene con error.
# MAGIC
# MAGIC Analogía: results es una lista de compras con un producto por renglón. Sprints es una carta escrita en varios renglones. Con `multiLine` le dices a Spark que lea la carta completa antes de interpretarla.

# COMMAND ----------

# DBTITLE 1,Leer sprints
sprints_df = (spark.read
  .format("json")
  .schema(sprints_schema)
  .option('multiLine', True)
  .option('mode', 'FAILFAST')
  .load(source_file)
)
display(sprints_df)

# COMMAND ----------

# DBTITLE 1,Paso 4: Metadatos
# MAGIC %md
# MAGIC ### Paso 4: Agregar columnas de auditoría
# MAGIC `add_ingestion_metadata` agrega `ingestion_timestamps` (cuándo cargamos) y `source_file` (de qué archivo salió cada fila).

# COMMAND ----------

# DBTITLE 1,Agregar metadatos
sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

# DBTITLE 1,Paso 5: Escribir
# MAGIC %md
# MAGIC ### Paso 5: Guardar en bronze
# MAGIC Formato Delta, `overwrite` para que puedas volver a correrlo sin duplicar, y `saveAsTable` para crear `formula1.bronze.sprints` en Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Escribir tabla bronze
(
    sprints_final_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

# COMMAND ----------

# DBTITLE 1,Paso 6: Verificar
# MAGIC %md
# MAGIC ### Paso 6: Comprobar
# MAGIC Esperado: **500 filas** en total, temporadas 2021-2025.
# MAGIC
# MAGIC **Ojo, problema conocido de los datos:** el sprint de São Paulo 2021 viene **duplicado** en el archivo (40 filas para 20 pilotos), y 7 de esas copias vienen sin `season` ni `round`. Por eso verás un grupo `season = NULL` con 7 filas.
# MAGIC
# MAGIC En bronze lo dejamos **tal cual llega**, porque bronze es la copia fiel del origen. La limpieza (quitar duplicados y descartar filas sin season) se hace en la capa **silver**.

# COMMAND ----------

# DBTITLE 1,Verificar carga
bronze_sprints = spark.table(table_name)
print("Filas totales:", bronze_sprints.count())
display(bronze_sprints.groupBy("season").count().orderBy("season"))
