# Databricks notebook source
# MAGIC %md
# MAGIC ## DS-2002: Sample Capstone Project (using Azure SQL)
# MAGIC This notebook demonstrates many of the software libraries and programming techniques required to fulfill the requirements of the final end-of-session capstone project for course **DS-2002: Data Systems** at the University of Virginia School of Data Science. The spirit of the project is to provide a capstone challenge that requires students to demonstrate a practical and functional understanding of each of the data systems and architectural principles covered throughout the session.
# MAGIC
# MAGIC **These include:**
# MAGIC - Relational Database Management Systems (e.g., MySQL, Microsoft SQL Server, Oracle, IBM DB2)
# MAGIC   - Online Transaction Processing Systems (OLTP): *Relational Databases Optimized for High-Volume Write Operations; Normalized to 3rd Normal Form.*
# MAGIC   - Online Analytical Processing Systems (OLAP): *Relational Databases Optimized for Read/Aggregation Operations; Dimensional Model (i.e, Star Schema)*
# MAGIC - NoSQL *(Not Only SQL)* Systems (e.g., MongoDB, CosmosDB, Cassandra, HBase, Redis)
# MAGIC - File System *(Data Lake)* Source Systems (e.g., AWS S3, Microsoft Azure Data Lake Storage)
# MAGIC   - Various Datafile Formats (e.g., JSON, CSV, Parquet, Text, Binary)
# MAGIC - Massively Parallel Processing *(MPP)* Data Integration Systems (e.g., Apache Spark, Databricks)
# MAGIC - Data Integration Patterns (e.g., Extract-Transform-Load, Extract-Load-Transform, Extract-Load-Transform-Load, Lambda & Kappa Architectures)
# MAGIC
# MAGIC What's more, this project requires students to make effective decisions regarding whether to implement a Cloud-hosted, on-premises hosted, or hybrid architecture.
# MAGIC
# MAGIC ### Section I: Prerequisites
# MAGIC
# MAGIC #### 1.0. Import Required Libraries

# COMMAND ----------

import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Instantiate Global Variables

# COMMAND ----------

# Azure SQL Server Connection Information #####################
jdbc_hostname = "wna8fw-sql.database.windows.net"
jdbc_port = 1433
src_database = "AdventureWorksLT"

connection_properties = {
  "user" : "jtupitza",
  "password" : "Passw0rd123",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "sandbox"
atlas_database_name = "adventure_works"
atlas_user_name = "m001-student"
atlas_password = "Passw0rd1234"

# Data Files (JSON) Information ###############################
dst_database = "adventure_works"

base_dir = "dbfs:/FileStore/ds2002-capstone"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_sales_orders/bronze"
output_silver = f"{database_dir}/fact_sales_orders/silver"
output_gold   = f"{database_dir}/fact_sales_orders/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_sales_orders", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Define Global Functions

# COMMAND ----------

# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl = f"jdbc:sqlserver://{host_name}:{port};database={db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
# ######################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.zibbf.mongodb.net/{db_name}?retryWrites=true&w=majority"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.zibbf.mongodb.net/{db_name}?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section II: Populate Dimensions by Ingesting Reference (Cold-path) Data 
# MAGIC #### 1.0. Fetch Reference Data From an Azure SQL Database
# MAGIC ##### 1.1. Create a New Databricks Metadata Database, and then Create a New Table that Sources its Data from a View in an Azure SQL database.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS adventure_works CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS adventure_works
# MAGIC COMMENT "Capstone Project Database"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/adventure_works"
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Capstone Project");

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_product
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://wna8fw-sql.database.windows.net:1433;database=AdventureWorksLT",
# MAGIC   dbtable "SalesLT.vDimProducts",
# MAGIC   user "jtupitza",
# MAGIC   password "Passw0rd123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE adventure_works;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adventure_works.dim_product
# MAGIC COMMENT "Products Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/adventure_works/dim_product"
# MAGIC AS SELECT * FROM view_product

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works.dim_product LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED adventure_works.dim_product;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Create a New Table that Sources its Data from a Table in an Azure SQL database. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW csv_bronze
# MAGIC USING CSV
# MAGIC OPTIONS(
# MAGIC   path = f"{batch_dir}/AdventureWorksLT_DimAddress.csv" ,
# MAGIC   header = True,
# MAGIC   inferSchema = True
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_date
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds2002-sql.database.windows.net:1433;database=AdventureWorksLT",
# MAGIC   dbtable "dbo.DimDate",
# MAGIC   user "jtupitza",
# MAGIC   password "P@ssw0rd123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE adventure_works;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adventure_works.dim_date
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/adventure_works/dim_date"
# MAGIC AS SELECT * FROM view_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works.dim_date LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED adventure_works.dim_date;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Fetch Reference Data from a MongoDB Atlas Database
# MAGIC ##### 2.1. View the Data Files on the Databricks File System

# COMMAND ----------

display(dbutils.fs.ls(batch_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Create a New MongoDB Database, and Load JSON Data Into a New MongoDB Collection
# MAGIC **NOTE:** The following cell **can** be run more than once because the **set_mongo_collection()** function **is** idempotent.

# COMMAND ----------

source_dir = '/dbfs/FileStore/ds2002-capstone/source_data/batch'
json_files = {"customers" : 'AdventureWorksLT_DimCustomer.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3. Fetch Data from the New MongoDB Collection

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC
# MAGIC val df_customer = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "adventure_works").option("collection", "customers").load()
# MAGIC display(df_customer)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4. Use the Spark DataFrame to Create a New Table in the Databricks (Adventure Works) Metadata Database

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customer.write.format("delta").mode("overwrite").saveAsTable("adventure_works.dim_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED adventure_works.dim_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.5. Query the New Table in the Databricks Metadata Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works.dim_customer LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Fetch Data from a File System
# MAGIC ##### 3.1. Use PySpark to Read From a CSV File

# COMMAND ----------

address_csv = f"{batch_dir}/AdventureWorksLT_DimAddress.csv"

df_address = spark.read.format('csv').options(header='true', inferSchema='true').load(address_csv)
display(df_address)

# COMMAND ----------

df_address.printSchema()

# COMMAND ----------

df_address.write.format("delta").mode("overwrite").saveAsTable("adventure_works.dim_address")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED adventure_works.dim_address;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works.dim_address LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE adventure_works;
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section III: Integrate Reference Data with Real-Time Data
# MAGIC #### 6.0. Use AutoLoader to Process Streaming (Hot Path) Data 
# MAGIC ##### 6.1. Bronze Table: Process 'Raw' JSON Data

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaHints", "SalesOrderID INT")
 .option("cloudFiles.schemaHints", "RevisionNumber TINYINT")
 .option("cloudFiles.schemaHints", "OrderDate TIMESTAMP")
 .option("cloudFiles.schemaHints", "DueDate TIMESTAMP") 
 .option("cloudFiles.schemaHints", "ShipDate TIMESTAMP")
 .option("cloudFiles.schemaHints", "Status TINYINT")
 .option("cloudFiles.schemaHints", "OnlineOrderFlag BINARY")
 .option("cloudFiles.schemaHints", "SalesOrderNumber STRING")
 .option("cloudFiles.schemaHints", "PurchaseOrderNumber STRING") 
 .option("cloudFiles.schemaHints", "AccountNumber STRING")
 .option("cloudFiles.schemaHints", "CustomerID INT")
 .option("cloudFiles.schemaHints", "ShipToAddressID INT")
 .option("cloudFiles.schemaHints", "BillToAddressID INT")
 .option("cloudFiles.schemaHints", "ShipMethod STRING")
 .option("cloudFiles.schemaHints", "SubTotal FLOAT")
 .option("cloudFiles.schemaHints", "TaxAmt FLOAT")
 .option("cloudFiles.schemaHints", "Freight FLOAT")
 .option("cloudFiles.schemaHints", "TotalDue FLOAT")
 .option("cloudFiles.schemaHints", "SalesOrderDetailID INT")
 .option("cloudFiles.schemaHints", "OrderQty SMALLINT")
 .option("cloudFiles.schemaHints", "ProductID INT")
 .option("cloudFiles.schemaHints", "UnitPrice FLOAT")
 .option("cloudFiles.schemaHints", "UnitPriceDiscount FLOAT")
 .option("cloudFiles.schemaHints", "LineTotal DECIMAL")
 .option("cloudFiles.schemaHints", "rowguid STRING")
 .option("cloudFiles.schemaHints", "ModifiedDate TIMESTAMP")
 .option("cloudFiles.schemaLocation", output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(stream_dir)
 .createOrReplaceTempView("orders_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM orders_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze_tempview

# COMMAND ----------

(spark.table("orders_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.2. Silver Table: Include Reference Data

# COMMAND ----------

(spark.readStream
  .table("fact_orders_bronze")
  .createOrReplaceTempView("orders_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_orders_silver_tempview AS (
# MAGIC   SELECT t.SalesOrderID
# MAGIC     , t.RevisionNumber
# MAGIC     , t.OrderDate
# MAGIC     , t.DueDate
# MAGIC     , t.ShipDate
# MAGIC     , t.Status
# MAGIC     , t.OnlineOrderFlag
# MAGIC     , t.SalesOrderNumber
# MAGIC     , t.PurchaseOrderNumber
# MAGIC     , t.AccountNumber
# MAGIC     , c.CustomerID
# MAGIC     , c.FirstName
# MAGIC     , c.LastName
# MAGIC     , t.ShipToAddressID
# MAGIC     , sa.AddressLine1 AS ShipToAddressLine1
# MAGIC     , sa.AddressLine2 AS ShipToAddressLine2
# MAGIC     , sa.City AS ShipToCity
# MAGIC     , sa.StateProvince AS ShipToStateProvince
# MAGIC     , sa.PostalCode AS ShipToPostalCode
# MAGIC     , t.BillToAddressID
# MAGIC     , ba.AddressLine1 AS BillToAddressLine1
# MAGIC     , ba.AddressLine2 AS BillToAddressLine2
# MAGIC     , ba.City AS BillToCity
# MAGIC     , ba.StateProvince AS BillToStateProvince
# MAGIC     , ba.PostalCode AS BillToPostalCode
# MAGIC     , t.ShipMethod
# MAGIC     , t.SubTotal
# MAGIC     , t.TaxAmt
# MAGIC     , t.Freight
# MAGIC     , t.TotalDue
# MAGIC     , t.SalesOrderDetailID
# MAGIC     , t.OrderQty
# MAGIC     , p.ProductID
# MAGIC     , p.ProductNumber
# MAGIC     , t.UnitPrice
# MAGIC     , t.UnitPriceDiscount
# MAGIC     , t.LineTotal
# MAGIC     , t.rowguid
# MAGIC     , t.ModifiedDate
# MAGIC     , t.receipt_time
# MAGIC     , t.source_file
# MAGIC   FROM orders_silver_tempview t
# MAGIC   INNER JOIN adventure_works.dim_customer c
# MAGIC   ON t.CustomerID = c.CustomerID
# MAGIC   INNER JOIN adventure_works.dim_address sa
# MAGIC   ON t.ShipToAddressID = CAST(sa.AddressID AS BIGINT)
# MAGIC   INNER JOIN adventure_works.dim_address ba
# MAGIC   ON t.BillToAddressID = CAST(ba.AddressID AS BIGINT)
# MAGIC   INNER JOIN adventure_works.dim_product p
# MAGIC   ON t.ProductID = p.ProductID)

# COMMAND ----------

(spark.table("fact_orders_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED adventure_works.fact_orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.4. Gold Table: Perform Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID
# MAGIC   , LastName
# MAGIC   , FirstName
# MAGIC   , COUNT(ProductID) AS ProductCount
# MAGIC FROM adventure_works.fact_orders_silver
# MAGIC GROUP BY CustomerID, LastName, FirstName
# MAGIC ORDER BY ProductCount DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pc.CustomerID
# MAGIC   , os.LastName AS CustomerName
# MAGIC   , os.ProductNumber
# MAGIC   , pc.ProductCount
# MAGIC FROM adventure_works.fact_orders_silver AS os
# MAGIC INNER JOIN (
# MAGIC   SELECT CustomerID
# MAGIC   , COUNT(ProductID) AS ProductCount
# MAGIC   FROM adventure_works.fact_orders_silver
# MAGIC   GROUP BY CustomerID
# MAGIC ) AS pc
# MAGIC ON pc.CustomerID = os.CustomerID
# MAGIC ORDER BY ProductCount DESC