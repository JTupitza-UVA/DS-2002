# Databricks notebook source
# MAGIC %md
# MAGIC ### Connectivity Examples
# MAGIC This notebook demonstrates extracting data from remote data sources like Azure SQL and MongoDB Atlas

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prerequisites
# MAGIC ##### Import Libraries

# COMMAND ----------

import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define Global Functions

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
# MAGIC #### 1.0. Connect to Azure SQL Server
# MAGIC ##### 1.1. First, validate that the correct driver is installed on your Databricks cluster.

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Declare and Instantiate Variables for Connection Properties

# COMMAND ----------

jdbcHostname = "wna8fw-sql.database.windows.net"
jdbcDatabase = "AdventureWorksLT"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase) 

connectionProperties = {
  "user" : "jtupitza",
  "password" : "Passw0rd123",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3. Execute a SQL Query Remotely on the Azure SQL Server

# COMMAND ----------

sql_query = """
(SELECT c.CustomerID
    , c.NameStyle
    , c.Title
    , c.FirstName
    , c.MiddleName
    , c.LastName
    , c.Suffix
    , c.CompanyName
    , c.SalesPerson
    , c.EmailAddress
    , c.Phone
    , ca.AddressType
    , a.AddressLine1
    , a.AddressLine2
    , a.City
    , a.StateProvince
    , a.CountryRegion
    , a.PostalCode
    , c.rowguid
    , c.ModifiedDate
FROM [SalesLT].[Customer] AS c
INNER JOIN [SalesLT].[CustomerAddress] AS ca
ON ca.CustomerID = c.CustomerID
INNER JOIN [SalesLT].[Address] AS a
ON a.AddressID = ca.AddressID) customer_dim
"""

df = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.4. Connect to Azure SQL Using Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DimProductModel
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://wna8fw-sql.database.windows.net:1433;database=AdventureWorksLT",
# MAGIC   dbtable "SalesLT.ProductModel",
# MAGIC   user "jtupitza",
# MAGIC   password "Passw0rd123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DimProductModel

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Connect to MongoDB Atlas
# MAGIC
# MAGIC ##### 2.1. Using the Spark MongoDB Library

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC
# MAGIC val df_trips = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "sample_training").option("collection", "trips").load()
# MAGIC display(df_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Using the PyMongo Library

# COMMAND ----------

import pymongo

atlas_cluster_name = "sandbox"
atlas_default_dbname = "sample_airbnb"
atlas_user_name = "m001-student"
atlas_password = "Passw0rd1234"

conn_str = f"mongodb+srv://{atlas_user_name}:{atlas_password}@{atlas_cluster_name}.zibbf.mongodb.net/{atlas_default_dbname}?retryWrites=true&w=majority"

client = pymongo.MongoClient(conn_str)
client.list_database_names()

# COMMAND ----------

db_name = "sample_training"

db = client[db_name]
db.list_collection_names()

# COMMAND ----------

collection = "trips"

trips = db[collection]
trips.find_one()

# COMMAND ----------

import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.

# The SELECT list -----------------------------------------------
projection = {"_id": 0, "start station location": 0, "end station location": 0}

# The WHERE clause ----------------------------------------------
conditions = {"tripduration":{"$gt": 90, "$lt": 100}, "birth year":{"$gte": 1970}}

# The ORDER BY clause -------------------------------------------
orderby = [("tripduration", -1)]

df = pd.DataFrame( list( db[collection].find(conditions, projection).sort(orderby) ) )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.0. Connect to MySQL
# MAGIC Connecting to MySQL would be identical to the Azure SQL database connection demonstrated in the code listings above.  The only differences would be the connection string variables (e.g., jdbc:mysql, jdbcPort = 3306) and the connection properties (e.g., driver: "com.mysql.jdbc.Driver", user, and password).

# COMMAND ----------

jdbcHostname = "wna8fw-mysql.mysql.database.azure.com"
jdbcDatabase = "northwind"
jdbcPort = 3306
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
  "user" : "jtupitza",
  "password" : "Passw0rd123",
  "driver" : "org.mariadb.jdbc.Driver"
}

# COMMAND ----------

query = "select * from customer;"
df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.0. Fetch Data from a File System
# MAGIC ##### 5.1. Use PySpark to Read From a CSV File Use 

# COMMAND ----------

address_csv = "dbfs:/FileStore/ds3002-capstone/data/batch/AdventureWorksLT_DimAddress.csv"

df_address = spark.read.format('csv').options(header='true', inferSchema='true').load(address_csv)
display(df_address)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works.dim_address LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5.2. Spark SQL to Read From a JSON file 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dim_address;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adventure_works.dim_address
# MAGIC USING json
# MAGIC OPTIONS (path="/FileStore/ds3002-capstone/data/batch/AdventureWorksLT_DimAddress.json", multiline=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works.dim_address LIMIT 5;

# COMMAND ----------

src_dbname = "adventure_works"
src_dir = '/dbfs/FileStore/ds3002-data'
json_files = {"customers" : 'AdventureWorksLT_DimCustomer.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, src_dbname, src_dir, json_files)

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC
# MAGIC val df_customer = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "adventure_works").option("collection", "customers").load()
# MAGIC display(df_customer)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customer.write.mode("overwrite").saveAsTable("adventure_works.dim_customer")