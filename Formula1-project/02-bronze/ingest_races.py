# Databricks notebook source
# DBTITLE 1,Descripción del notebook
# MAGIC %md
# MAGIC ### Ingesta de races.csv a la capa bronze
# MAGIC 1. Leer el archivo CSV con el DataFrame reader de Spark (usando un esquema definido)
# MAGIC 2. Agregar columnas de auditoría: fecha/hora de ingesta y archivo de origen
# MAGIC 3. Guardar el resultado como tabla Delta en `formula1.bronze.races`

# COMMAND ----------

# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

# DBTITLE 1,Explicación: esquema de races
# MAGIC %md
# MAGIC ### Paso 1 - Definir el esquema de races
# MAGIC **Qué queremos hacer:** decirle a Spark de antemano qué columnas tiene `races.csv` y de qué tipo es cada una: `IntegerType` (números enteros, como la temporada), `StringType` (texto) y `DateType` (fechas).
# MAGIC
# MAGIC **Por qué:** sin esquema, Spark tendría que "adivinar" los tipos leyendo el archivo, lo que es más lento y puede equivocarse (por ejemplo, leer la fecha como texto).
# MAGIC
# MAGIC **Nota:** la columna `circuitId` es la que conecta cada carrera con su circuito (tabla `formula1.bronze.circuits`).

# COMMAND ----------

# DBTITLE 1,Esquema de races
# Importo los tipos de datos necesarios para el esquema de races
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# Defino el esquema de races.csv
races_schema = StructType([
    StructField('season',    IntegerType()),  # temporada (año)
    StructField('round',     IntegerType()),  # número de carrera dentro de la temporada
    StructField('url',       StringType()),   # enlace a Wikipedia
    StructField('raceName',  StringType()),   # nombre del Gran Premio
    StructField('date',      DateType()),     # fecha de la carrera
    StructField('circuitId', StringType())    # circuito donde se corrió (relaciona con circuits)
])

# COMMAND ----------

# DBTITLE 1,Explicación: leer races.csv
# MAGIC %md
# MAGIC ### Paso 2 - Leer el archivo races.csv
# MAGIC **Qué queremos hacer:** cargar el CSV que está en el volumen de *landing* (la "bandeja de entrada" de archivos crudos) a un DataFrame de Spark.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `spark.read.format('csv')`: indica que vamos a leer un CSV.
# MAGIC * `.schema(races_schema)`: aplica el esquema definido en el paso anterior.
# MAGIC * `.load(..., header=True)`: lee el archivo y usa la primera fila como nombres de columna.

# COMMAND ----------

# DBTITLE 1,Leer races.csv
# Leo races.csv desde el volumen de landing con el esquema definido
races_df = (
    spark.read
    .format('csv')
    .schema(races_schema)
    .load(source_file, header=True)
)

# COMMAND ----------

# DBTITLE 1,Explicación: revisar races
# MAGIC %md
# MAGIC ### Paso 3 - Revisar los datos leídos
# MAGIC **Qué queremos hacer:** comprobar que las columnas y los tipos (por ejemplo, que `date` se lea como fecha) son correctos.
# MAGIC
# MAGIC **Qué hace el código:** `display()` muestra el DataFrame como una tabla interactiva en el notebook.

# COMMAND ----------

# DBTITLE 1,Ver datos leídos de races
# Muestro el DataFrame para revisar la lectura
display(races_df)

# COMMAND ----------

# DBTITLE 1,Explicación: auditoría races
# MAGIC %md
# MAGIC ### Paso 4 - Agregar columnas de auditoría
# MAGIC **Qué queremos hacer:** dejar registro de *cuándo* y *desde dónde* se cargó cada fila, como un sello con la fecha de llegada y el remitente.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `ingestion_timestamps`: guarda la fecha y hora actual con `current_timestamp()`.
# MAGIC * `source_file`: guarda la ruta del archivo de origen, tomada de la columna oculta `_metadata`.

# COMMAND ----------

# DBTITLE 1,Agregar columnas de auditoría a races
# Importo las funciones de Spark SQL con el alias f
from pyspark.sql import functions as f

# Agrego columnas de auditoría: fecha/hora de ingesta y archivo de origen
races_final_df = (
    races_df
    .withColumn('ingestion_timestamps', f.current_timestamp())
    .withColumn('source_file', f.col('_metadata.file_path'))
)

# COMMAND ----------

# DBTITLE 1,Explicación: guardar races
# MAGIC %md
# MAGIC ### Paso 5 - Guardar en la capa bronze
# MAGIC **Qué queremos hacer:** guardar el resultado como tabla Delta en `formula1.bronze.races`.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `.format("delta")`: guarda en formato Delta (similar a una tabla en Snowflake).
# MAGIC * `.mode("overwrite")`: si la tabla ya existe, reemplaza su contenido, así se puede re-ejecutar sin duplicar.
# MAGIC * `.saveAsTable(...)`: registra la tabla en Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Guardar races en bronze
# Guardo races como tabla Delta en la capa bronze (sobrescribe si ya existe)
(
    races_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_name)
)

# COMMAND ----------

# DBTITLE 1,Explicación: consultar races
# MAGIC %md
# MAGIC ### Paso 6 - Consultar la tabla creada
# MAGIC **Qué queremos hacer:** validar con SQL que `formula1.bronze.races` se creó correctamente.

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Consultar tabla bronze races
# MAGIC %sql
# MAGIC -- Consulto la tabla creada para confirmar que se guardó correctamente
# MAGIC SELECT * FROM formula1.bronze.races
