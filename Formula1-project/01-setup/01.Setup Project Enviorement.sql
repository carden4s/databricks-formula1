-- Databricks notebook source
-- DBTITLE 1,Titulo
-- MAGIC %md
-- MAGIC # Setup del proyecto Formula 1
-- MAGIC
-- MAGIC Crea la ubicación externa (External Location) en Unity Catalog que da acceso al contenedor de ADLS Gen2 donde se guardan los datos de Formula 1, usando la credencial `databricks-course-sc`.

-- COMMAND ----------

-- DBTITLE 1,Crear external location de Formula 1
-- Ubicacion externa para los datos de Formula 1
-- Ajusta el contenedor (formula1) si en Azure tiene otro nombre
CREATE EXTERNAL LOCATION IF NOT EXISTS formula1_ext_location
  URL 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `databricks-course-sc`)
  COMMENT 'Ubicacion externa para los datos del proyecto Formula 1';

-- COMMAND ----------

-- DBTITLE 1,Verificar la external location
-- Revisa la configuracion de la ubicacion externa
DESCRIBE EXTERNAL LOCATION formula1_ext_location;

-- COMMAND ----------

-- DBTITLE 1,Listar archivos del contenedor
-- Prueba de acceso: lista los archivos del contenedor
LIST 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/landing'

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

create catalog if not exists formula1
    managed location 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/'
    comment 'This is the main catalog for the formula 1 project'

-- COMMAND ----------

create schema if not exists formula1.landing
;

create schema if not exists formula1.bronze
    managed location 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/bronze'
    comment 'This is the bronze schema for the formula 1 project';
create schema if not exists formula1.silver
    managed location 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/silver'
    comment 'This is the silver schema for the formula 1 project';
create schema if not exists formula1.gold
    managed location 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/gold'
    comment 'This is the gold schema for the formula 1 project';

-- COMMAND ----------

show schemas

-- COMMAND ----------

Use catalog formula1

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
  LOCATION 'abfss://formula1@databrickscourseextdlx.dfs.core.windows.net/landing'


-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files
