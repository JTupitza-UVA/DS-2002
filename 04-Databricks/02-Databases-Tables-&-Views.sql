-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creating Databases, Tables, and Views
-- MAGIC An important feature of Databricks is its supprot for multiple languages including Python, Scala, R, and Spark SQL.  Of particular importance, Databricks enables reading data from various file formats (e.g., text, CSV, JSON, Parquet) into familiar database table structures using the **Spark SQL** language. This capability exemplifies the **Schema on Read** data storage and retrieval paradigm wherein data is read from an underlying storage device in a non-destructive manner in order to expose that data to consumers as if it were stored in a relational database management system (RDBMS) like SQL Server, Oracle or MySQL.  While a number of file formats are supported, this approach is most useful for reading data from self-describing data formats like JSON or Parquet.
-- MAGIC 
-- MAGIC #### 1.0. Import Shared Utilities and Data Files
-- MAGIC 
-- MAGIC This lab will demonstrate ingesting data collected from weather stations (e.g., average temperatures in Fahrenheit and Celsius) that's been written in the **Parquet** format for the sake of illustrating how **Spark SQL** can be used to create and explore interactions between various relational entities (e.g., databases, tables, and views). The schema for the table is as follows:
-- MAGIC 
-- MAGIC |ColumnName  | DataType| Description|
-- MAGIC |------------|---------|------------|
-- MAGIC |NAME        |string   | Station name |
-- MAGIC |STATION     |string   | Unique ID |
-- MAGIC |LATITUDE    |float    | Latitude |
-- MAGIC |LONGITUDE   |float    | Longitude |
-- MAGIC |ELEVATION   |float    | Elevation |
-- MAGIC |DATE        |date     | YYYY-MM-DD |
-- MAGIC |UNIT        |string   | Temperature units |
-- MAGIC |TAVG        |float    | Average temperature |
-- MAGIC 
-- MAGIC First, run the following cell to import the data and make various utilities available for our experimentation.

-- COMMAND ----------

-- MAGIC %run ./Includes/2.0-setup

-- COMMAND ----------

SELECT * FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.0. Creating a Database
-- MAGIC 
-- MAGIC Create a database in the default location using the **`da.db_name`** variable defined in setup script, and then switch execution context to in with the **USE** clause.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name};

USE ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.0. Creating Tables
-- MAGIC Here we will demonstrate creating both **managed** and **external** tables.  These differ in that an *external table* specifies the storage location; whereas the *managed table* does not. A **managed table** is a Spark SQL table where Spark manages both the data and the metadata by storing them in the DBFS account of the user. In this case, if the user were to execute a DROP TABLE statement then Spark would delete *both the metadata and the data!* 
-- MAGIC 
-- MAGIC The other option, an **external table** (also known as an **unmanaged table**) allows Spark to manage the metadata while the user specifies the storage location for the data.  This approach is especially useful when the source data is located in a shared enterprise Data Lake; thereby making it desirable to enable querying the data *in-place* rather than causing any undesired data movement.  In this case, if the user were to execute a DROP TABLE statement then Spark would delete *only the metadata* and *not* the data itself; i.e., the data would remain in the storage location.   
-- MAGIC 
-- MAGIC #####3.1. Creating a Managed Table
-- MAGIC Here we demonstrate using a CTAS (Create Table As Select) statement to create a managed table named **`weather_managed`**.

-- COMMAND ----------

CREATE TABLE weather_managed AS
  SELECT * FROM parquet.`${da.paths.working_dir}/weather`;
  
DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.2. Creating an External Table
-- MAGIC 
-- MAGIC Recall that an external table differs from a managed table through specification of a location. Create an external table called **`weather_external`** below.

-- COMMAND ----------

CREATE TABLE weather_external
  LOCATION "${da.paths.working_dir}/lab/external"
  AS SELECT * FROM parquet.`${da.paths.working_dir}/weather`;
  
DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.3. Compare the Storage Location of Each Table
-- MAGIC Run the following helper code to extract and compare the table locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]
-- MAGIC 
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC print(f"""The weather_managed table is saved at: {managedTablePath}""")
-- MAGIC 
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC print(f"""The weather_external table is saved at: {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC List the contents of these directories to confirm that data exists in both locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managed_files = dbutils.fs.ls(managedTablePath)
-- MAGIC display(managed_files)
-- MAGIC 
-- MAGIC external_files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(external_files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.4. Check Directory Contents after Dropping Database and All Tables
-- MAGIC The **`CASCADE`** keyword will accomplish this.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With the database dropped, the files will have been deleted as well.
-- MAGIC 
-- MAGIC Uncomment and run the following cell, which will throw a **`FileNotFoundException`** as your confirmation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # files = dbutils.fs.ls(managedTablePath)
-- MAGIC # display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(DA.paths.working_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **This highlights the main differences between managed and external tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
-- MAGIC 
-- MAGIC Files for external tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.0. Create a Database with a Specified Path
-- MAGIC 
-- MAGIC Assuming you dropped your database in the last step, you can use the same **`database`** name.

-- COMMAND ----------

CREATE DATABASE ${da.db_name}
  LOCATION '${da.paths.working_dir}/${da.db_name}';

USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Recreate your **`weather_managed`** table in this new database and print out the location of this table.

-- COMMAND ----------

CREATE TABLE weather_managed AS
  SELECT * FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While here we're using the **`userhome`** directory created on the DBFS root, _any_ object store can be used as the database directory. **Defining database directories for groups of users can greatly reduce the chances of accidental data exfiltration**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 5.0. Creating Views
-- MAGIC 
-- MAGIC Here we will demonstrate using the **`AS`** clause to register a view named **`celsius`**, a temporary view named **`celsius_temp`**, and a global temp view named **`celsius_global`**.

-- COMMAND ----------

CREATE OR REPLACE VIEW celsius AS
(
  SELECT * FROM weather_managed
  WHERE UNIT = "C"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now create a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW celsius_temp AS
(
  SELECT * FROM weather_managed
  WHERE UNIT = "C"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now register a global temp view.

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW celsius_global AS 
(
  SELECT * FROM weather_managed
  WHERE UNIT = "C"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Views will be displayed alongside tables when listing from the catalog.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note the following:
-- MAGIC - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
-- MAGIC - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
-- MAGIC - The global temp view does not appear in our catalog. **Global temp views will always register to the **`global_temp`** database**. The **`global_temp`** database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While no job was triggered when defining these views, a job is triggered _each time_ a query is executed against the view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 6.0. Clean Up
-- MAGIC Drop the database and all tables to clean up your workspace.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
