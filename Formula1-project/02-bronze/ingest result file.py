# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Race Results
# MAGIC

# COMMAND ----------

# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------

# MAGIC %run ../01-setup/01.bronze-helpers

# COMMAND ----------

# DBTITLE 1,Paso 1: Origen y destino
# MAGIC %md
# MAGIC ### Paso 1: ¿De dónde leo y adónde escribo?
# MAGIC * `source_file`: apunta a la **carpeta** `results` (no a un archivo). Spark lee todos los `results_YYYY.json` que haya dentro de una sola vez, como un `COPY INTO` de Snowflake sobre un stage completo.
# MAGIC * `table_name`: el nombre completo de la tabla en Unity Catalog, con el formato `catalogo.esquema.tabla` → `formula1.bronze.results`. Es igual que `database.schema.table` en Snowflake.
# MAGIC * `landing_folder_path`, `catalog_name` y `bronze_schema` vienen del notebook `01.enviorement-config` que cargamos con `%run`.

# COMMAND ----------

# DBTITLE 1,Origen y destino
source_file = f"{landing_folder_path}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# DBTITLE 1,Paso 2: Esquema
# MAGIC %md
# MAGIC ### Paso 2: Definir el esquema (los "planos" de la tabla)
# MAGIC Le decimos a Spark qué columnas esperamos y de qué tipo son. Así:
# MAGIC * No tiene que "adivinar" los tipos leyendo todos los archivos (es más rápido).
# MAGIC * Si un archivo trae algo raro, nos enteramos enseguida en vez de guardar datos malos.
# MAGIC
# MAGIC Es como el `CREATE TABLE (...)` que escribes en Snowflake antes de cargar datos. Las columnas salen de los JSON (cada línea es un piloto en una carrera).

# COMMAND ----------

# DBTITLE 1,Esquema de results
results_schema = """
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
# MAGIC ### Paso 3: Leer los archivos JSON
# MAGIC * `spark.read.format("json")`: vamos a leer archivos JSON (uno por línea).
# MAGIC * `.schema(results_schema)`: usa el esquema del paso 2.
# MAGIC * `.option('mode', 'FAILFAST')`: si alguna fila no encaja con el esquema, **para todo con error**. Es mejor fallar que cargar basura en silencio.
# MAGIC * `.load(source_file)`: lee la carpeta completa → los 76 años (1950-2025) en un solo DataFrame.
# MAGIC
# MAGIC Un DataFrame es como el resultado de un `SELECT`: una tabla en memoria que todavía no se guardó en ningún lado.

# COMMAND ----------

# DBTITLE 1,Leer results
results_df = (spark.read
  .format("json")
  .schema(results_schema)
  .option('mode', 'FAILFAST')
  .load(source_file)
)
display(results_df)

# COMMAND ----------

# DBTITLE 1,Paso 4: Metadatos
# MAGIC %md
# MAGIC ### Paso 4: Agregar columnas de auditoría
# MAGIC Usamos la función `add_ingestion_metadata` de `01.bronze-helpers`. Agrega dos columnas:
# MAGIC * `ingestion_timestamps`: cuándo cargamos los datos.
# MAGIC * `source_file`: de qué archivo salió cada fila (por ejemplo `.../results_1998.json`). Como aquí juntamos 76 archivos, esta columna es muy útil para saber de qué año salió cada fila si hay algún problema.

# COMMAND ----------

# DBTITLE 1,Agregar metadatos
results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

# DBTITLE 1,Paso 5: Escribir
# MAGIC %md
# MAGIC ### Paso 5: Guardar en la capa bronze
# MAGIC * `.format("delta")`: guarda la tabla en formato Delta (el formato de tablas de Databricks; tiene historial y transacciones, parecido a las tablas internas de Snowflake).
# MAGIC * `.mode("overwrite")`: si la tabla ya existe, la **reemplaza** completa. Así puedes correr el notebook varias veces sin duplicar filas.
# MAGIC * `.saveAsTable(table_name)`: la crea como tabla en Unity Catalog → `formula1.bronze.results`.

# COMMAND ----------

# DBTITLE 1,Escribir tabla bronze
(
    results_final_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

# COMMAND ----------

# DBTITLE 1,Paso 6: Verificar
# MAGIC %md
# MAGIC ### Paso 6: Comprobar que quedó bien
# MAGIC Leemos la tabla recién creada y contamos filas por temporada. Deberías ver 76 temporadas (1950-2025) y unas 25,907 filas en total.

# COMMAND ----------

# DBTITLE 1,Verificar carga
bronze_results = spark.table(table_name)
print("Filas totales:", bronze_results.count())
display(bronze_results.groupBy("season").count().orderBy("season"))

# COMMAND ----------


