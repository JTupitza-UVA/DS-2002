-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Querying Files Directly with Databricks
-- MAGIC Databricks enables reading data from various file formats into RDBMS-like table constructs using the **Spark SQL** language. This capability examplifies the *Schema on Read* data storage and retrieval paradigm wherein data is read from an underlying storage device in a non-destructive manner in order to expose that data to consumers as if it were stored in a relational database management system like SQL Server, Oracle or MySQL. While a number of file formats are supported, this approach is most useful for reading data from self-describing data formats like JSON or Parquet. 
-- MAGIC 
-- MAGIC #### 1.0. Import Shared Utilities and Data Files
-- MAGIC This lab will demonstrate ingesting data from a sample of **Apache Kafka** data that's been written to JSON files.  Apache Kafka is a distributed event store and stream-processing platform; therefore, this data represents what would be expected from a *Streaming* data source.  Each file contains records consumed in a 5-minute interval, stored with the full Kafka schema as a multi-record JSON file.
-- MAGIC 
-- MAGIC | field | type | description |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | timestamp | LONG | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC 
-- MAGIC First, run the following cell to import the data and make various utilities available for our experimentation.

-- COMMAND ----------

-- MAGIC %run ./Includes/1.0-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Then, run the following cell to verify the existence of each of the data files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path = f"{DA.paths.datasets}/raw/events-kafka"
-- MAGIC print(dataset_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(dataset_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.0. Querying a Single File
-- MAGIC For the sake of this lab, the data has been saved to the DBFS (Databricks File System) root that's created for you when you provision a Databricks Workspace; however, most production environments will have the source data stored on some cloud storage location that's external to the Databricks runtime environment.
-- MAGIC 
-- MAGIC Querying data that's stored in a single data file is as simple as executing the following Spark SQL query.  **NOTE:** The delimiters in the following query  are *back-ticks*. They are NOT single-quotes!

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.0. Query Multiple Files in the Same Directory
-- MAGIC When all the files in a directory are of the same format and schema they can be queried simultaneously by simply specifying the *path to the directory* rather than the *path to an individual file*; however, the result-set is limited to the first 1000 rows by default.  What's more, we can create a **view** using a query that specifies a directory path so that all files in the directory may be accessed by subsequent queries simply by referencing the view.

-- COMMAND ----------

CREATE OR REPLACE VIEW events_view AS
  SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;
  
SELECT * FROM events_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.0. Extracting Data as Strings, Raw Bytes, and File Metadata
-- MAGIC Text-based file formats like CSV, JSON and TXT can all be read using the **text** format which will load each line in the form of a **row** having only one string-typed column named **value**.  When working with **binary** data, such as when processing *image* files or other *unstructured* file types, the *binary* representation of the data and its acccompanying *metadata* can be read using the **binaryFile** format. This will return columns named **path, modificationTime, length,** and **content**. 

-- COMMAND ----------

SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------


