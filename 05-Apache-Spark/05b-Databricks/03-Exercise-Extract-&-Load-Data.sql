-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Extract and Load Data Lab
-- MAGIC In this lab, you will extract and load raw data from JSON files into a Delta table.
-- MAGIC 
-- MAGIC #### Objectives
-- MAGIC - Create an external table to extract data from JSON files
-- MAGIC - Create an empty Delta table with a provided schema
-- MAGIC - Insert records from an existing table into a Delta table
-- MAGIC - Use a CTAS statement to create a Delta table from files
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

-- MAGIC %run ./Includes/3.0-Lab-setup

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### 2.0 Extract Raw Events From JSON Files
-- MAGIC To load this data into Delta properly, we first need to extract the JSON data using the correct schema.  Create an external table against JSON files located at the filepath provided below. Name this table **`events_json`** and declare the schema above.

-- COMMAND ----------

path = "${da.paths.datasets}/raw/events-kafka"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return an error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC 
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- MAGIC %md #### 3.0. Insert Raw Events Into Delta Table
-- MAGIC ##### 3.1. Create an empty managed Delta table named **`events_raw`** using the same schema.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw"), "Table named `events_raw` does not exist"
-- MAGIC assert spark.table("events_raw").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_raw").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC assert spark.table("events_raw").count() == 0, "The table should have 0 records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.2. Once the extracted data and Delta table are ready, **`insert`** the JSON records from the **`events_json`** table into the new **`events_raw`** Delta table.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3.3. Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to confirm the data has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw").count() == 2252, "The table should have 2252 records"
-- MAGIC assert set(row['timestamp'] for row in spark.table("events_raw").select("timestamp").limit(5).collect()) == {1593880885085, 1593880892303, 1593880889174, 1593880886106, 1593880889725}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md #### 4.0 Create Delta Table from a Query
-- MAGIC In addition to new events data, let's also load a small lookup table that provides product details that we'll use later in the course.
-- MAGIC Use a CTAS statement to create a managed Delta table named **`item_lookup`** that extracts data from the **`parquet`** directory provided below. 

-- COMMAND ----------

${da.paths.datasets}/raw/item-lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to confirm the lookup table has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("item_lookup").count() == 12, "The table should have 12 records"
-- MAGIC assert set(row['item_id'] for row in spark.table("item_lookup").select("item_id").limit(5).collect()) == {'M_PREM_F', 'M_PREM_K', 'M_PREM_Q', 'M_PREM_T', 'M_STAN_F'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
