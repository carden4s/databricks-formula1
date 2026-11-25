# Databricks notebook source
# DBTITLE 1,Descripción del notebook
# MAGIC %md
# MAGIC ### Ingesta de circuits.csv y races.csv a la capa bronze
# MAGIC 1. Leer los archivos CSV con el DataFrame reader de Spark (usando un esquema definido)
# MAGIC 2. Agregar columnas de auditoría: fecha/hora de ingesta y archivo de origen
# MAGIC 3. Guardar el resultado como tabla Delta en `formula1.bronze`

# COMMAND ----------

# DBTITLE 1,Explicación: esquema de circuits
# MAGIC %md
# MAGIC ## Parte 1: Circuits
# MAGIC ### Paso 1.1 - Definir el esquema de circuits
# MAGIC **Qué queremos hacer:** decirle a Spark de antemano qué columnas tiene `circuits.csv` y de qué tipo es cada una (texto, número decimal, etc.).
# MAGIC
# MAGIC **Por qué:** si no le damos un esquema, Spark tiene que leer el archivo para "adivinar" los tipos (`inferSchema`), lo cual es más lento y puede equivocarse. Es como entregar un formulario con los campos ya etiquetados en lugar de que alguien tenga que deducir qué va en cada casilla.
# MAGIC
# MAGIC **Qué hace el código:** crea un `StructType` (la estructura de la tabla) con un `StructField` por cada columna.

# COMMAND ----------

# DBTITLE 1,Esquema de circuits
# Importo los tipos de datos para definir el esquema manualmente
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Defino el esquema de circuits.csv: nombre de cada columna y su tipo.
# Así Spark no tiene que "adivinar" los tipos (inferSchema) y la lectura es más rápida y confiable.
circuit_schema = StructType([
    StructField('circuitId',   StringType()),  # identificador del circuito
    StructField('url',         StringType()),  # enlace a Wikipedia
    StructField('circuitName', StringType()),  # nombre del circuito
    StructField('lat',         DoubleType()),  # latitud
    StructField('long',        DoubleType()),  # longitud
    StructField('locality',    StringType()),  # ciudad
    StructField('country',     StringType())   # país
])

# COMMAND ----------

# DBTITLE 1,Explicación: leer circuits.csv
# MAGIC %md
# MAGIC ### Paso 1.2 - Leer el archivo circuits.csv
# MAGIC **Qué queremos hacer:** cargar el CSV que está en el volumen de *landing* (la "bandeja de entrada" donde llegan los archivos crudos) a un DataFrame de Spark.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `spark.read.format('csv')`: indica que vamos a leer un CSV.
# MAGIC * `.schema(circuit_schema)`: aplica el esquema definido en el paso anterior.
# MAGIC * `.load(..., header=True)`: lee el archivo y usa la primera fila como nombres de columna.

# COMMAND ----------

# DBTITLE 1,Leer circuits.csv
# Leo el CSV desde el volumen de landing aplicando el esquema definido.
# header=True indica que la primera fila contiene los nombres de las columnas.
circuit_df = (
    spark.read
    .format('csv')
    .schema(circuit_schema)
    .load('/Volumes/formula1/landing/files/circuits.csv', header=True)
)

# COMMAND ----------

# DBTITLE 1,Explicación: revisar circuits
# MAGIC %md
# MAGIC ### Paso 1.3 - Revisar los datos leídos
# MAGIC **Qué queremos hacer:** comprobar visualmente que los datos se cargaron bien (columnas correctas, sin valores raros o vacíos).
# MAGIC
# MAGIC **Qué hace el código:** `display()` muestra el DataFrame como una tabla interactiva en el notebook.

# COMMAND ----------

# DBTITLE 1,Ver datos leídos de circuits
# Muestro el DataFrame para revisar que los datos se leyeron bien
display(circuit_df)

# COMMAND ----------

# DBTITLE 1,Explicación: auditoría circuits
# MAGIC %md
# MAGIC ### Paso 1.4 - Agregar columnas de auditoría
# MAGIC **Qué queremos hacer:** dejar registro de *cuándo* y *desde dónde* se cargó cada fila. Es como ponerle a cada paquete un sello con la fecha de llegada y el remitente: si algo sale mal, sabemos de dónde vino.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `ingestion_timestamps`: guarda la fecha y hora actual con `current_timestamp()`.
# MAGIC * `source_file`: guarda la ruta del archivo de origen, tomada de la columna oculta `_metadata` que Spark agrega al leer archivos.

# COMMAND ----------

# DBTITLE 1,Agregar columnas de auditoría a circuits
# Importo las funciones de Spark SQL con el alias f
from pyspark.sql import functions as f

# Agrego columnas de auditoría:
#  - ingestion_timestamps: fecha y hora en que se cargó el dato
#  - source_file: ruta del archivo de origen (sale de la columna oculta _metadata)
circuits_final_df = (
    circuit_df
    .withColumn('ingestion_timestamps', f.current_timestamp())
    .withColumn('source_file', f.col('_metadata.file_path'))
)

# COMMAND ----------

# DBTITLE 1,Explicación: verificar auditoría circuits
# MAGIC %md
# MAGIC ### Paso 1.5 - Verificar las columnas nuevas
# MAGIC **Qué queremos hacer:** confirmar que `ingestion_timestamps` y `source_file` aparecen con valores correctos antes de guardar.

# COMMAND ----------

# DBTITLE 1,Ver circuits con auditoría
# Verifico que las nuevas columnas se hayan agregado correctamente
display(circuits_final_df)

# COMMAND ----------

# DBTITLE 1,Explicación: guardar circuits
# MAGIC %md
# MAGIC ### Paso 1.6 - Guardar en la capa bronze
# MAGIC **Qué queremos hacer:** persistir los datos como tabla Delta en `formula1.bronze.circuits`, para que queden disponibles para las siguientes capas (silver/gold).
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `.format("delta")`: guarda en formato Delta (similar a una tabla en Snowflake: soporta transacciones, historial de versiones y actualizaciones).
# MAGIC * `.mode("overwrite")`: si la tabla ya existe, reemplaza su contenido. Así podemos re-ejecutar el notebook sin duplicar datos.
# MAGIC * `.saveAsTable(...)`: registra la tabla en Unity Catalog con el nombre `catalogo.esquema.tabla`.

# COMMAND ----------

# DBTITLE 1,Guardar circuits en bronze
# Guardo el DataFrame como tabla Delta administrada en Unity Catalog.
# mode("overwrite") reemplaza el contenido si la tabla ya existe (la carga se puede re-ejecutar sin duplicar).
(
    circuits_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("formula1.bronze.circuits")
)

# COMMAND ----------

# DBTITLE 1,Explicación: consultar circuits
# MAGIC %md
# MAGIC ### Paso 1.7 - Consultar la tabla creada
# MAGIC **Qué queremos hacer:** validar con SQL que la tabla existe y tiene los datos esperados.

# COMMAND ----------

# DBTITLE 1,Consultar tabla bronze circuits
# MAGIC %sql
# MAGIC -- Consulto la tabla creada para confirmar que se guardó correctamente
# MAGIC SELECT * FROM formula1.bronze.circuits

# COMMAND ----------

# DBTITLE 1,Explicación: esquema de races
# MAGIC %md
# MAGIC ## Parte 2: Races
# MAGIC Repetimos el mismo proceso que con circuits, ahora para `races.csv`.
# MAGIC
# MAGIC ### Paso 2.1 - Definir el esquema de races
# MAGIC **Qué queremos hacer:** indicar las columnas de `races.csv` y sus tipos. Aquí aparecen tipos nuevos: `IntegerType` (números enteros, como la temporada) y `DateType` (fechas).
# MAGIC
# MAGIC **Nota:** la columna `circuitId` es la que conecta cada carrera con su circuito de la Parte 1.

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
# MAGIC ### Paso 2.2 - Leer el archivo races.csv
# MAGIC **Qué queremos hacer:** cargar `races.csv` desde el volumen de landing aplicando el esquema definido, igual que en el paso 1.2.

# COMMAND ----------

# DBTITLE 1,Leer races.csv
# Leo races.csv desde el volumen de landing con el esquema definido
races_df = (
    spark.read
    .format('csv')
    .schema(races_schema)
    .load('/Volumes/formula1/landing/files/races.csv', header=True)
)

# COMMAND ----------

# DBTITLE 1,Explicación: revisar races
# MAGIC %md
# MAGIC ### Paso 2.3 - Revisar los datos leídos
# MAGIC **Qué queremos hacer:** comprobar que las columnas y los tipos (por ejemplo, que `date` se lea como fecha) son correctos.

# COMMAND ----------

# DBTITLE 1,Ver datos leídos de races
# Muestro el DataFrame para revisar la lectura
display(races_df)

# COMMAND ----------

# DBTITLE 1,Explicación: auditoría races
# MAGIC %md
# MAGIC ### Paso 2.4 - Agregar columnas de auditoría
# MAGIC **Qué queremos hacer:** agregar las mismas columnas de control que en circuits (`ingestion_timestamps` y `source_file`) para saber cuándo y desde qué archivo se cargó cada fila.

# COMMAND ----------

# DBTITLE 1,Agregar columnas de auditoría a races
from pyspark.sql import functions as f

# Agrego las mismas columnas de auditoría que en circuits:
# fecha/hora de ingesta y archivo de origen
races_final_df = (
    races_df
    .withColumn('ingestion_timestamps', f.current_timestamp())
    .withColumn('source_file', f.col('_metadata.file_path'))
)

# COMMAND ----------

# DBTITLE 1,Explicación: guardar races
# MAGIC %md
# MAGIC ### Paso 2.5 - Guardar en la capa bronze
# MAGIC **Qué queremos hacer:** guardar el resultado como tabla Delta en `formula1.bronze.races`, sobrescribiendo si ya existe.

# COMMAND ----------

# DBTITLE 1,Guardar races en bronze
# Guardo races como tabla Delta en la capa bronze (sobrescribe si ya existe)
(
    races_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("formula1.bronze.races")
)

# COMMAND ----------

# DBTITLE 1,Explicación: consultar races
# MAGIC %md
# MAGIC ### Paso 2.6 - Consultar la tabla creada
# MAGIC **Qué queremos hacer:** validar con SQL que `formula1.bronze.races` se creó correctamente.

# COMMAND ----------

# DBTITLE 1,Consultar tabla bronze races
# MAGIC %sql
# MAGIC -- Consulto la tabla creada para confirmar que se guardó correctamente
# MAGIC SELECT * FROM formula1.bronze.races
