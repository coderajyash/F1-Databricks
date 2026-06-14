-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Setup Project Environment
-- MAGIC - Setup External location
-- MAGIC - Create Catalog formula 1
-- MAGIC - Create Schemas for landing, bronze, silver and gold
-- MAGIC - Create Volume files for landing schema

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databricksworkingext.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksworkingext_formula1
    URL 'abfss://formula1@databricksworkingext.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databricks-working-sg`)
    COMMENT 'Creating Storage Credentials for formula 1 container';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databricksworkingext.dfs.core.windows.net/'

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

CREATE CATALOG  IF NOT EXISTS  formula1
    MANAGED LOCATION 'abfss://formula1@databricksworkingext.dfs.core.windows.net/'
    COMMENT 'Creating a catalog'

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
-- Landing does not have schema as it has raw files so no managed location defined
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@databricksworkingext.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@databricksworkingext.dfs.core.windows.net/silver';

CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@databricksworkingext.dfs.core.windows.net/gold';


-- COMMAND ----------

USE CATALOG formula1;
SHOW SCHEMAS;

-- COMMAND ----------

CREATE  EXTERNAL  VOLUME  IF NOT EXISTS formula1.landing.files
    LOCATION 'abfss://formula1@databricksworkingext.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Volumes creation helps in easily accessing the files from external location without using the abfss method

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files

-- COMMAND ----------

