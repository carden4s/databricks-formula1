# Databricks notebook source
# DBTITLE 1,Descripción del notebook
# MAGIC %md
# MAGIC ### Transformación de circuits: capa bronze → capa silver
# MAGIC
# MAGIC **Requisitos:**
# MAGIC 1. Leer la tabla bronze `formula1.bronze.circuits`
# MAGIC 2. Conservar solo las columnas necesarias para análisis (eliminar la columna `url`)
# MAGIC 3. Estandarizar los nombres de columnas a *snake_case* (`circuitId` → `circuit_id`, `circuitName` → `circuit_name`)
# MAGIC 4. Renombrar columnas para que sean más descriptivas (`lat` → `latitude`, `long` → `longitude`)
# MAGIC 5. Filtrar las filas donde `circuit_id` es nulo (validación de la clave de negocio)
# MAGIC 6. Eliminar registros duplicados
# MAGIC 7. Transformar los valores de `circuit_name` y `locality` a formato título (*Title Case*)
# MAGIC 8. Escribir los datos transformados en la tabla silver `formula1.silver.circuits`
# MAGIC
# MAGIC **Analogía:** la capa bronze es la "bodega" donde guardamos los datos tal cual llegaron. La capa silver es la "vitrina": los mismos datos, pero limpios, ordenados y listos para que los analistas los usen.

# COMMAND ----------

# DBTITLE 1,Cargar configuración del entorno
# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------

# DBTITLE 1,Definir tablas de origen y destino
# Construyo los nombres completos (catalogo.esquema.tabla) usando las variables del notebook de configuracion.
# Asi, si cambia el catalogo o el esquema, solo se modifica en un lugar.
source_table = f"{catalog_name}.{bronze_schema}.circuits"   # origen: formula1.bronze.circuits
target_table = f"{catalog_name}.{silver_schema}.circuits"   # destino: formula1.silver.circuits

# COMMAND ----------

# DBTITLE 1,Explicación: leer bronze
# MAGIC %md
# MAGIC ### Paso 1 - Leer la tabla bronze de circuits
# MAGIC **Qué queremos hacer:** cargar en un DataFrame los datos que dejamos en la capa bronze.
# MAGIC
# MAGIC **Por qué:** la capa silver siempre parte de bronze. Bronze es nuestra "fuente de verdad" en crudo; nunca la modificamos, solo leemos de ella.
# MAGIC
# MAGIC **Qué hace el código:** `spark.read.table()` lee una tabla registrada en Unity Catalog. Es el equivalente a hacer `SELECT * FROM formula1.bronze.circuits` en Snowflake, pero el resultado queda en un DataFrame sobre el que podemos seguir aplicando transformaciones.

# COMMAND ----------

# DBTITLE 1,Leer bronze circuits
# Importo las funciones de Spark SQL con el alias f (las usaremos en varios pasos)
from pyspark.sql import functions as f

# Leo la tabla bronze completa en un DataFrame
circuits_df = spark.read.table(source_table)

# Reviso los datos tal como llegan desde bronze
display(circuits_df)

# COMMAND ----------

# DBTITLE 1,Explicación: eliminar url
# MAGIC %md
# MAGIC ### Paso 2 - Conservar solo las columnas necesarias (eliminar `url`)
# MAGIC **Qué queremos hacer:** quitar la columna `url` (enlace a Wikipedia), porque no aporta nada al análisis.
# MAGIC
# MAGIC **Por qué:** menos columnas = tablas más livianas y más fáciles de entender. Es como sacar de la mochila lo que no vas a usar en el viaje.
# MAGIC
# MAGIC **Qué hace el código:** `.drop('url')` devuelve un DataFrame nuevo sin esa columna (en Snowflake sería `SELECT * EXCLUDE (url)`). Las columnas de auditoría (`ingestion_timestamps`, `source_file`) se mantienen para conservar la trazabilidad.

# COMMAND ----------

# DBTITLE 1,Eliminar columna url
# Elimino la columna url, que no se necesita para analisis
circuits_selected_df = circuits_df.drop("url")

# COMMAND ----------

# DBTITLE 1,Explicación: snake_case
# MAGIC %md
# MAGIC ### Paso 3 - Estandarizar nombres a *snake_case*
# MAGIC **Qué queremos hacer:** pasar `circuitId` → `circuit_id` y `circuitName` → `circuit_name`.
# MAGIC
# MAGIC **Por qué:** *snake_case* (palabras en minúsculas separadas por `_`) es la convención más usada en tablas de datos. Si todas las tablas siguen la misma regla, nadie tiene que adivinar si una columna se llama `circuitId`, `CircuitID` o `circuit_id`.
# MAGIC
# MAGIC **Qué hace el código:** `.withColumnsRenamed()` recibe un diccionario `{nombre_viejo: nombre_nuevo}` y renombra varias columnas de una sola vez (equivale a `SELECT circuitId AS circuit_id, ...`).

# COMMAND ----------

# DBTITLE 1,Renombrar a snake_case
# Renombro las columnas en camelCase a snake_case
circuits_renamed_df = circuits_selected_df.withColumnsRenamed({
    "circuitId":   "circuit_id",
    "circuitName": "circuit_name"
})

# COMMAND ----------

# DBTITLE 1,Explicación: nombres descriptivos
# MAGIC %md
# MAGIC ### Paso 4 - Renombrar columnas a nombres más descriptivos
# MAGIC **Qué queremos hacer:** cambiar `lat` → `latitude` y `long` → `longitude`.
# MAGIC
# MAGIC **Por qué:** las abreviaturas son ambiguas (`long` puede confundirse con "largo" o con el tipo de dato *long*). Un nombre completo se entiende sin tener que preguntar.
# MAGIC
# MAGIC **Qué hace el código:** igual que el paso anterior, usa `.withColumnsRenamed()` con un diccionario.

# COMMAND ----------

# DBTITLE 1,Renombrar lat y long
# Renombro las coordenadas con nombres completos y descriptivos
circuits_renamed_df = circuits_renamed_df.withColumnsRenamed({
    "lat":  "latitude",
    "long": "longitude"
})

# COMMAND ----------

# DBTITLE 1,Explicación: filtrar nulos
# MAGIC %md
# MAGIC ### Paso 5 - Filtrar filas sin `circuit_id` (validación de la clave de negocio)
# MAGIC **Qué queremos hacer:** descartar los registros donde `circuit_id` es nulo.
# MAGIC
# MAGIC **Por qué:** `circuit_id` es la **clave de negocio**: el dato que identifica a cada circuito y que otras tablas (por ejemplo `races`) usan para unirse con esta. Una fila sin clave es como un paquete sin etiqueta de destino: no se puede saber a quién pertenece. En bronze hay 2 filas en esta situación.
# MAGIC
# MAGIC **Qué hace el código:** `.filter(f.col('circuit_id').isNotNull())` conserva solo las filas con valor (equivale a `WHERE circuit_id IS NOT NULL`).

# COMMAND ----------

# DBTITLE 1,Filtrar circuit_id nulo
# Conservo solo las filas que tienen clave de negocio (circuit_id no nulo)
circuits_valid_df = circuits_renamed_df.where(f.col("circuit_id").isNotNull())

# COMMAND ----------

# DBTITLE 1,Explicación: duplicados
# MAGIC %md
# MAGIC ### Paso 6 - Eliminar registros duplicados
# MAGIC **Qué queremos hacer:** dejar una sola copia de las filas que vienen repetidas.
# MAGIC
# MAGIC **Por qué:** los duplicados inflan los conteos y generan resultados incorrectos al hacer joins (un circuito repetido duplicaría sus carreras). En bronze, `dijon` e `indianapolis` aparecen dos veces con exactamente los mismos valores.
# MAGIC
# MAGIC **Qué hace el código:** `.dropDuplicates()` sin argumentos elimina las filas idénticas en todas las columnas (equivale a `SELECT DISTINCT *`).

# COMMAND ----------

# DBTITLE 1,Eliminar duplicados
# Elimino las filas exactamente duplicadas
circuits_dedup_df = circuits_valid_df.dropDuplicates()

# COMMAND ----------

display( circuits_dedup_df)

# COMMAND ----------

# DBTITLE 1,Explicación: title case
# MAGIC %md
# MAGIC ### Paso 7 - Formato título en `circuit_name` y `locality`
# MAGIC **Qué queremos hacer:** convertir textos como `circuit gilles villeneuve` en `Circuit Gilles Villeneuve`, y `montreal` en `Montreal`.
# MAGIC
# MAGIC **Por qué:** en bronze los nombres vienen todos en minúsculas. En reportes y dashboards se ven mucho mejor con la primera letra de cada palabra en mayúscula.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `f.initcap()` pone en mayúscula la primera letra de cada palabra y el resto en minúscula (en Snowflake también existe `INITCAP`).
# MAGIC * `.withColumns()` recibe un diccionario `{columna: nueva_expresión}` y reemplaza ambas columnas de una vez.

# COMMAND ----------

# DBTITLE 1,Aplicar title case
# Aplico formato titulo (primera letra de cada palabra en mayuscula)
circuits_final_df = circuits_dedup_df.withColumns({
    "circuit_name": f.initcap(f.col("circuit_name")),
    "locality":     f.initcap(f.col("locality"))
})

# Reviso el resultado final antes de guardarlo
display(circuits_final_df)

# COMMAND ----------

# DBTITLE 1,Explicación: guardar en silver
# MAGIC %md
# MAGIC ### Paso 8 - Guardar en la capa silver
# MAGIC **Qué queremos hacer:** persistir el resultado como tabla Delta en `formula1.silver.circuits`.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `.format('delta')`: guarda en formato Delta (como una tabla de Snowflake: con transacciones e historial de versiones).
# MAGIC * `.mode('overwrite')`: reemplaza el contenido si la tabla ya existe, así el notebook se puede re-ejecutar sin duplicar datos.
# MAGIC * `.saveAsTable(...)`: registra la tabla en Unity Catalog para poder consultarla por su nombre.

# COMMAND ----------

# DBTITLE 1,Guardar circuits en silver
# Guardo el DataFrame transformado como tabla Delta administrada en la capa silver
(
    circuits_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)

# COMMAND ----------

# DBTITLE 1,Explicación: validar silver
# MAGIC %md
# MAGIC ### Paso 9 - Validar la tabla silver
# MAGIC **Qué queremos hacer:** comprobar con SQL que la tabla quedó bien: sin `circuit_id` nulos, sin duplicados y con la cantidad de filas esperada.

# COMMAND ----------

# DBTITLE 1,Validar tabla silver circuits
# MAGIC %sql
# MAGIC -- Comparo el total de filas con la cantidad de circuit_id distintos (deben ser iguales si no hay duplicados)
# MAGIC -- y cuento cuantos circuit_id nulos quedaron (debe ser 0)
# MAGIC SELECT
# MAGIC   COUNT(*)                                        AS total_filas,
# MAGIC   COUNT(DISTINCT circuit_id)                      AS circuitos_distintos,
# MAGIC   COUNT_IF(circuit_id IS NULL)                    AS circuit_id_nulos
# MAGIC FROM formula1.silver.circuits

# COMMAND ----------

# DBTITLE 1,Consultar tabla silver circuits
# MAGIC %sql
# MAGIC -- Consulto la tabla silver para revisar el resultado final
# MAGIC SELECT * FROM formula1.silver.circuits
# MAGIC ORDER BY circuit_id

# COMMAND ----------

# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------


