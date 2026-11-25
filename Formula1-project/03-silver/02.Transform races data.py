# Databricks notebook source
# DBTITLE 1,Descripción del notebook
# MAGIC %md
# MAGIC ### Transformación de races: capa bronze → capa silver
# MAGIC
# MAGIC **Requisitos:**
# MAGIC 1. Leer la tabla bronze `formula1.bronze.races`
# MAGIC 2. Conservar solo las columnas necesarias para análisis (eliminar la columna `url`)
# MAGIC 3. Estandarizar los nombres de columnas a *snake_case* (`raceName` → `race_name`, `circuitId` → `circuit_id`)
# MAGIC 4. Renombrar columnas para que sean más descriptivas (`date` → `race_date`)
# MAGIC 5. Eliminar registros duplicados
# MAGIC 6. Transformar los valores de `race_name` a formato título (*Title Case*)
# MAGIC 7. Escribir los datos transformados en la tabla silver `formula1.silver.races`
# MAGIC
# MAGIC **Analogía:** igual que con circuits, bronze es la "bodega" donde guardamos los datos tal cual llegaron, y silver es la "vitrina": los mismos datos, pero limpios y listos para usar.
# MAGIC
# MAGIC **Funciones que vamos a usar:** `spark.read.table`, `display`, `.drop`, `.withColumnsRenamed`, `.dropDuplicates`, `.withColumn`, `f.initcap`, `f.col`, `.write...saveAsTable`. Cada una se explica en su paso.

# COMMAND ----------

# DBTITLE 1,Explicación: configuración
# MAGIC %md
# MAGIC ### Paso 0 - Cargar la configuración del proyecto
# MAGIC **Qué queremos hacer:** traer las variables comunes del proyecto (`catalog_name`, `bronze_schema`, `silver_schema`).
# MAGIC
# MAGIC **Por qué:** así no escribimos los nombres a mano en cada notebook. Si mañana cambia el catálogo, se cambia en un solo lugar.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `%run` ejecuta otro notebook dentro de este, como si copiaras y pegaras sus celdas aquí. Todas sus variables quedan disponibles.
# MAGIC * Con un *f-string* (`f"...{variable}..."`) armamos el nombre completo `catalogo.esquema.tabla`. Python reemplaza lo que está entre `{}` por el valor de la variable.

# COMMAND ----------

# DBTITLE 1,Cargar configuración del entorno
# MAGIC %run ../01-setup/01.enviorement-config

# COMMAND ----------

# DBTITLE 1,Definir tablas de origen y destino
# Construyo los nombres completos (catalogo.esquema.tabla) usando las variables del notebook de configuracion
source_table = f"{catalog_name}.{bronze_schema}.races"   # origen: formula1.bronze.races
target_table = f"{catalog_name}.{silver_schema}.races"   # destino: formula1.silver.races

# COMMAND ----------

# DBTITLE 1,Explicación: leer bronze
# MAGIC %md
# MAGIC ### Paso 1 - Leer la tabla bronze de races
# MAGIC **Qué queremos hacer:** cargar en un DataFrame las carreras que dejamos en la capa bronze.
# MAGIC
# MAGIC **Por qué:** silver siempre parte de bronze. Bronze no se modifica, solo se lee.
# MAGIC
# MAGIC **Funciones que usamos:**
# MAGIC * `from pyspark.sql import functions as f`: importa la caja de herramientas de funciones de Spark (`col`, `initcap`, etc.) con el apodo `f`. Así escribimos `f.initcap(...)` en vez de un nombre largo.
# MAGIC * `spark.read.table(nombre)`: lee una tabla de Unity Catalog y la devuelve como DataFrame. Es como `SELECT * FROM formula1.bronze.races` en Snowflake. Recuerda que Spark es *lazy*: aquí solo prepara la lectura, todavía no trae los datos.
# MAGIC * `display(df)`: muestra el DataFrame como una tabla interactiva. Aquí sí Spark lee los datos, porque le pedimos un resultado.

# COMMAND ----------

# DBTITLE 1,Leer bronze races
# Importo las funciones de Spark SQL con el alias f (las usaremos en varios pasos)
from pyspark.sql import functions as f

# Leo la tabla bronze completa en un DataFrame
races_df = spark.read.table(source_table)

# Reviso los datos tal como llegan desde bronze
display(races_df)

# COMMAND ----------

# DBTITLE 1,Explicación: eliminar url
# MAGIC %md
# MAGIC ### Paso 2 - Conservar solo las columnas necesarias (eliminar `url`)
# MAGIC **Qué queremos hacer:** quitar la columna `url` (el enlace a Wikipedia de cada carrera).
# MAGIC
# MAGIC **Por qué:** no sirve para el análisis y solo hace la tabla más pesada. Es como sacar de la mochila lo que no vas a usar.
# MAGIC
# MAGIC **Función que usamos:**
# MAGIC * `.drop("url")`: devuelve un DataFrame **nuevo** sin esa columna; el original no cambia. En Snowflake sería `SELECT * EXCLUDE (url)`.
# MAGIC
# MAGIC Las columnas de auditoría (`ingestion_timestamps`, `source_file`) se quedan, para saber de dónde vino cada fila.

# COMMAND ----------

# DBTITLE 1,Eliminar columna url
# Elimino la columna url, que no se necesita para analisis
races_selected_df = races_df.drop("url")

# COMMAND ----------

# DBTITLE 1,Explicación: snake_case
# MAGIC %md
# MAGIC ### Paso 3 - Estandarizar nombres a *snake_case*
# MAGIC **Qué queremos hacer:** pasar `raceName` → `race_name` y `circuitId` → `circuit_id`.
# MAGIC
# MAGIC **Por qué:** *snake_case* (minúsculas separadas por `_`) es la convención de todas nuestras tablas. Además, `circuit_id` queda con el mismo nombre que en `formula1.silver.circuits`, así es obvio que por esa columna se unen las dos tablas en un join.
# MAGIC
# MAGIC **Función que usamos:**
# MAGIC * `.withColumnsRenamed({"viejo": "nuevo", ...})`: recibe un diccionario y renombra varias columnas de una vez. En Snowflake sería `SELECT raceName AS race_name, ...`. Si una columna del diccionario no existe, simplemente se ignora.

# COMMAND ----------

# DBTITLE 1,Renombrar a snake_case
# Renombro las columnas en camelCase a snake_case
races_renamed_df = races_selected_df.withColumnsRenamed({
    "raceName":  "race_name",
    "circuitId": "circuit_id"
})

# COMMAND ----------

# DBTITLE 1,Explicación: nombres descriptivos
# MAGIC %md
# MAGIC ### Paso 4 - Renombrar columnas a nombres más descriptivos
# MAGIC **Qué queremos hacer:** cambiar `date` → `race_date`.
# MAGIC
# MAGIC **Por qué:** `date` es muy genérico (¿fecha de qué?) y además es una palabra reservada en SQL, lo que obliga a escribirla entre comillas invertidas en algunas consultas. `race_date` dice exactamente qué es.
# MAGIC
# MAGIC **Función que usamos:** otra vez `.withColumnsRenamed()`, ahora con una sola columna.

# COMMAND ----------

# DBTITLE 1,Renombrar date a race_date
# Renombro la fecha con un nombre mas descriptivo
races_renamed_df = races_renamed_df.withColumnsRenamed({
    "date": "race_date"
})

# COMMAND ----------

# DBTITLE 1,Explicación: duplicados
# MAGIC %md
# MAGIC ### Paso 5 - Eliminar registros duplicados
# MAGIC **Qué queremos hacer:** dejar una sola copia de las filas que vienen repetidas.
# MAGIC
# MAGIC **Por qué:** una carrera duplicada infla los conteos y, al hacer joins con resultados, duplica también esos resultados.
# MAGIC
# MAGIC **Función que usamos:**
# MAGIC * `.dropDuplicates()`: sin argumentos, compara **todas** las columnas y elimina las filas idénticas. Es como `SELECT DISTINCT *` en Snowflake. También acepta una lista de columnas (por ejemplo `["season", "round"]`) si quisieras quitar duplicados solo por la clave.
# MAGIC
# MAGIC En bronze hay 1178 filas y 5 son copias exactas, así que deberían quedar 1173.

# COMMAND ----------

# DBTITLE 1,Eliminar duplicados
# Elimino las filas exactamente duplicadas
races_dedup_df = races_renamed_df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Explicación: title case
# MAGIC %md
# MAGIC ### Paso 6 - Formato título en `race_name`
# MAGIC **Qué queremos hacer:** convertir `british grand prix` en `British Grand Prix`.
# MAGIC
# MAGIC **Por qué:** en bronze los nombres vienen en minúsculas; en reportes se ven mejor con mayúscula inicial.
# MAGIC
# MAGIC **Funciones que usamos:**
# MAGIC * `f.col("race_name")`: hace referencia a una columna del DataFrame (como escribir el nombre de la columna en un `SELECT`).
# MAGIC * `f.initcap(...)`: pone en mayúscula la primera letra de cada palabra y el resto en minúscula. En Snowflake es `INITCAP`.
# MAGIC * `.withColumn("race_name", expresión)`: crea una columna con ese nombre, o la **reemplaza** si ya existe. Aquí la usamos en singular porque solo cambiamos una columna (en circuits usamos `.withColumns()` porque eran dos).

# COMMAND ----------

# DBTITLE 1,Aplicar title case
# Aplico formato titulo (primera letra de cada palabra en mayuscula)
races_final_df = races_dedup_df.withColumn("race_name", f.initcap(f.col("race_name")))

# Reviso el resultado final antes de guardarlo
display(races_final_df)

# COMMAND ----------

# DBTITLE 1,Explicación: guardar en silver
# MAGIC %md
# MAGIC ### Paso 7 - Guardar en la capa silver
# MAGIC **Qué queremos hacer:** guardar el resultado como tabla Delta en `formula1.silver.races`.
# MAGIC
# MAGIC **Funciones que usamos:**
# MAGIC * `.write`: empieza la configuración de escritura del DataFrame.
# MAGIC * `.mode("overwrite")`: si la tabla ya existe, reemplaza su contenido. Así el notebook se puede volver a ejecutar sin duplicar datos.
# MAGIC * `.format("delta")`: guarda en formato Delta (con transacciones ACID e historial de versiones, como una tabla de Snowflake).
# MAGIC * `.saveAsTable(nombre)`: escribe los datos y registra la tabla en Unity Catalog, para consultarla por su nombre.
# MAGIC
# MAGIC Los paréntesis que envuelven el bloque solo sirven para poder escribir un método por línea.

# COMMAND ----------

# DBTITLE 1,Guardar races en silver
# Guardo el DataFrame transformado como tabla Delta administrada en la capa silver
(
    races_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(target_table)
)

# COMMAND ----------

# DBTITLE 1,Explicación: validar silver
# MAGIC %md
# MAGIC ### Paso 8 - Validar la tabla silver
# MAGIC **Qué queremos hacer:** comprobar con SQL que la tabla quedó bien.
# MAGIC
# MAGIC **Funciones que usamos:**
# MAGIC * `COUNT(*)`: total de filas (esperamos 1173).
# MAGIC * `COUNT(DISTINCT season, round)`: cuántas carreras únicas hay. Cada carrera se identifica por temporada + ronda, así que si es igual al total, no quedan duplicados.
# MAGIC * `COUNT_IF(condición)`: cuenta las filas que cumplen la condición. Lo usamos para ver cuántos nombres no quedaron en formato título (esperamos 0).

# COMMAND ----------

# DBTITLE 1,Validar tabla silver races
# MAGIC %sql
# MAGIC -- Total de filas vs. carreras unicas (season + round): deben ser iguales si no hay duplicados
# MAGIC -- y cuantos race_name no quedaron en formato titulo (debe ser 0)
# MAGIC SELECT
# MAGIC   COUNT(*)                                  AS total_filas,
# MAGIC   COUNT(DISTINCT season, round)             AS carreras_distintas,
# MAGIC   COUNT_IF(race_name <> INITCAP(race_name)) AS nombres_sin_formato
# MAGIC FROM formula1.silver.races

# COMMAND ----------

# DBTITLE 1,Consultar tabla silver races
# MAGIC %sql
# MAGIC -- Consulto la tabla silver para revisar el resultado final
# MAGIC SELECT * FROM formula1.silver.races
# MAGIC ORDER BY season, round
