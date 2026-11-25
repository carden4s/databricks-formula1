# Databricks notebook source
# DBTITLE 1,Título y descripción
# MAGIC %md
# MAGIC # Formula 1 - Diagrama entidad-relación (ERD)
# MAGIC
# MAGIC Modelo de datos del catálogo `formula1`: qué tablas hay, cuáles son sus claves y cómo se conectan entre sí.
# MAGIC
# MAGIC **¿Qué es un ERD?** Es el "mapa" de la base de datos. Cada caja es una tabla (entidad) y cada línea dice cómo se relacionan. Es como el árbol genealógico de los datos: te dice quién es "padre" de quién.
# MAGIC
# MAGIC **Conceptos clave:**
# MAGIC * **PK (Primary Key / clave primaria):** la columna (o columnas) que identifica a cada fila sin repetirse. Como el DNI de una persona.
# MAGIC * **FK (Foreign Key / clave foránea):** una columna que "apunta" a la PK de otra tabla. Es lo que usas en un `JOIN`.
# MAGIC * **Cardinalidad:** cuántos de un lado corresponden al otro. `||--o{` se lee "uno a muchos": un circuito tiene muchas carreras, pero cada carrera ocurre en un solo circuito.
# MAGIC
# MAGIC **Comparación con Snowflake:** igual que en Snowflake, en Databricks las PK y FK son **informativas**: documentan el modelo, pero el motor no impide que entren datos que las rompan. Por eso las validamos nosotros con SQL (al final de este notebook).

# COMMAND ----------

# DBTITLE 1,Explicación: el modelo
# MAGIC %md
# MAGIC ### Las tablas del modelo
# MAGIC
# MAGIC El modelo tiene forma de **estrella**: en el centro están las tablas de **hechos** (lo que pasó: resultados de cada piloto en cada carrera) y alrededor las **dimensiones** (quién, dónde, cuándo).
# MAGIC
# MAGIC | Tabla | Tipo | Qué guarda | Clave primaria |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `circuits` | Dimensión | Los circuitos (pistas) | `circuit_id` |
# MAGIC | `races` | Dimensión | Cada carrera de cada temporada | `season` + `round` (clave compuesta) |
# MAGIC | `drivers` | Dimensión | Los pilotos | `driver_id` |
# MAGIC | `constructors` | Dimensión | Los equipos (Ferrari, McLaren...) | `constructor_id` |
# MAGIC | `results` | Hechos | El resultado de cada piloto en cada Gran Premio | sin clave natural única (ver nota) |
# MAGIC | `sprints` | Hechos | El resultado de cada piloto en cada carrera sprint (desde 2021) | sin clave natural única |
# MAGIC
# MAGIC **Cómo se conectan:**
# MAGIC * Un **circuito** acoge muchas **carreras** → `races.circuit_id` apunta a `circuits.circuit_id`.
# MAGIC * Una **carrera** tiene muchos **resultados** y puede tener muchos **resultados de sprint** → `results.season + results.round` apuntan a `races.season + races.round`.
# MAGIC * Un **piloto** y un **equipo** aparecen en muchos resultados → `results.driver_id` y `results.constructor_id`.
# MAGIC
# MAGIC **Notas sobre los nombres:** el diagrama usa los nombres en *snake_case* que tendrán las tablas en silver. Hoy `circuits` y `races` ya están en silver; `drivers`, `constructors`, `results` y `sprints` todavía están solo en bronze con nombres *camelCase* (`driverId`, `constructorId`...). En `drivers`, el nombre viene en bronze como una estructura (`name.givenName`, `name.familyName`); en el diagrama aparece ya separado en dos columnas. Las columnas de auditoría (`ingestion_timestamps`, `source_file`) están en todas las tablas y no las dibujo para no recargar el diagrama.

# COMMAND ----------

# DBTITLE 1,Explicación: cómo se dibuja
# MAGIC %md
# MAGIC ### El diagrama
# MAGIC **Qué hace el código:**
# MAGIC * El diagrama está escrito en **Mermaid**, un lenguaje de texto para dibujar diagramas. Cada bloque `TABLA { tipo columna PK/FK }` es una caja, y cada línea como `CIRCUITS ||--o{ RACES` es una relación.
# MAGIC * Cómo leer los símbolos: `||` = exactamente uno, `o{` = cero o muchos. Así, `CIRCUITS ||--o{ RACES` se lee "un circuito tiene cero o muchas carreras".
# MAGIC * `displayHTML()` es una función de Databricks que muestra HTML dentro del notebook. Le pasamos el texto Mermaid y la librería que lo convierte en dibujo (se descarga de internet al ejecutar la celda).
# MAGIC
# MAGIC Para ver el diagrama, ejecuta la celda de abajo. Si en el futuro agregas una tabla, solo tienes que editar el texto del diagrama.

# COMMAND ----------

# DBTITLE 1,Dibujar ERD del esquema Formula 1
# Definicion del diagrama en lenguaje Mermaid (erDiagram)
# Formato de cada columna: tipo nombre [PK|FK] "comentario"
erd = """
erDiagram
    CIRCUITS ||--o{ RACES : "acoge"
    RACES ||--o{ RESULTS : "tiene"
    RACES ||--o{ SPRINTS : "tiene"
    DRIVERS ||--o{ RESULTS : "obtiene"
    DRIVERS ||--o{ SPRINTS : "obtiene"
    CONSTRUCTORS ||--o{ RESULTS : "obtiene"
    CONSTRUCTORS ||--o{ SPRINTS : "obtiene"

    CIRCUITS {
        string circuit_id PK "id del circuito"
        string circuit_name
        double latitude
        double longitude
        string locality
        string country
    }

    RACES {
        int season PK "temporada"
        int round PK "numero de carrera en la temporada"
        string race_name
        date race_date
        string circuit_id FK "circuits.circuit_id"
    }

    DRIVERS {
        string driver_id PK "id del piloto"
        string given_name "bronze: name.givenName"
        string family_name "bronze: name.familyName"
        date date_of_birth
        string nationality
    }

    CONSTRUCTORS {
        string constructor_id PK "id del equipo"
        string constructor_name
        string nationality
    }

    RESULTS {
        int season FK "races.season"
        int round FK "races.round"
        string driver_id FK "drivers.driver_id"
        string constructor_id FK "constructors.constructor_id"
        int number "numero del auto"
        int grid "posicion de salida"
        int position "posicion final"
        string position_text
        double points
        int laps
        string status
    }

    SPRINTS {
        int season FK "races.season"
        int round FK "races.round"
        string driver_id FK "drivers.driver_id"
        string constructor_id FK "constructors.constructor_id"
        int number
        int grid
        int position
        string position_text
        double points
        int laps
        string status
    }
"""

# HTML que carga la libreria Mermaid y dibuja el diagrama
html = (
    '<div class="mermaid">' + erd + '</div>'
    '<script type="module">'
    "import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';"
    "mermaid.initialize({ startOnLoad: true, theme: 'default' });"
    '</script>'
)

# Muestro el diagrama dentro del notebook
displayHTML(html)

# COMMAND ----------

# DBTITLE 1,Explicación: validar relaciones
# MAGIC %md
# MAGIC ### Validar las relaciones con los datos reales
# MAGIC **Qué queremos hacer:** comprobar que las flechas del diagrama se cumplen de verdad. Como las FK son informativas, nadie lo garantiza por nosotros.
# MAGIC
# MAGIC **Qué hace el código:**
# MAGIC * `LEFT ANTI JOIN`: devuelve las filas de la tabla izquierda que **no** encuentran pareja en la derecha. Son las filas "huérfanas" (por ejemplo, un resultado cuya carrera no existe). En Snowflake se haría con `WHERE NOT EXISTS (...)`.
# MAGIC * `UNION ALL`: junta varias consultas en un solo resultado, una fila por comprobación.
# MAGIC * Lo ideal es que `huerfanos` sea 0 en todas las filas.
# MAGIC
# MAGIC Uso bronze para las tablas que todavía no tienen versión silver.

# COMMAND ----------

# DBTITLE 1,Validar claves foráneas
# MAGIC %sql
# MAGIC -- Cuento filas huerfanas por cada relacion del diagrama (lo ideal es 0)
# MAGIC SELECT 'races -> circuits' AS relacion, COUNT(*) AS huerfanos
# MAGIC FROM formula1.silver.races r
# MAGIC LEFT ANTI JOIN formula1.silver.circuits c ON r.circuit_id = c.circuit_id
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'results -> races', COUNT(*)
# MAGIC FROM formula1.bronze.results x
# MAGIC LEFT ANTI JOIN formula1.silver.races r ON x.season = r.season AND x.round = r.round
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'results -> drivers', COUNT(*)
# MAGIC FROM formula1.bronze.results x
# MAGIC LEFT ANTI JOIN formula1.bronze.drivers d ON x.driverId = d.driverId
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'results -> constructors', COUNT(*)
# MAGIC FROM formula1.bronze.results x
# MAGIC LEFT ANTI JOIN formula1.bronze.constructors c ON x.constructorId = c.constructorId
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'sprints -> races', COUNT(*)
# MAGIC FROM formula1.bronze.sprints x
# MAGIC LEFT ANTI JOIN formula1.silver.races r ON x.season = r.season AND x.round = r.round

# COMMAND ----------

# DBTITLE 1,Hallazgos
# MAGIC %md
# MAGIC ### Lo que encontramos al validar
# MAGIC * **`races -> circuits`, `results -> drivers` y `results -> constructors`: 0 huérfanos.** Estas relaciones se cumplen perfectamente.
# MAGIC * **`results -> races`: 11 huérfanos.** Son filas de results que llegan sin `season` o sin `round` (nulos). Hay que filtrarlas en silver, igual que hicimos con `circuit_id` nulo en circuits.
# MAGIC * **`sprints -> races`: 7 huérfanos.** Son las copias del sprint de São Paulo 2021 que vienen sin `season` ni `round` (ya lo habíamos visto en la ingesta de sprints).
# MAGIC * **`results` no tiene una clave única natural.** Uno pensaría que `season + round + driver_id` identifica cada fila, pero no: en los años 50 un piloto podía compartir o cambiar de auto en la misma carrera y aparecer dos veces. Por eso en el diagrama results no tiene PK, solo FK. Si más adelante hace falta una, se puede crear una clave técnica (un id generado).
# MAGIC * **Columnas repetidas en results/sprints:** en bronze traen también `raceName`, `date` y `url`, que ya están en races. En silver conviene quitarlas y obtenerlas con un join a races (así no se guarda el mismo dato en dos lugares).
