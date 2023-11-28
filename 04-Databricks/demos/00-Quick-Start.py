# Databricks notebook source
# MAGIC %md
# MAGIC ### Getting Started with Databricks Data Engineering
# MAGIC There are three key Apache Spark interfaces that you should know about: Resilient Distributed Dataset, DataFrame, and Dataset.
# MAGIC
# MAGIC - **Resilient Distributed Dataset:** The first Apache Spark abstraction was the Resilient Distributed Dataset (RDD). It is an interface to a sequence of data objects that consist of one or more types that are located across a collection of machines (a cluster). RDDs can be created in a variety of ways and are the “lowest level” API available. While this is the original data structure for Apache Spark, you should focus on the DataFrame API, which is a superset of the RDD functionality. The RDD API is available in the Java, Python, and Scala languages.
# MAGIC - **DataFrame:** These are similar in concept to the DataFrame you may be familiar with in the pandas Python library and the R language. The DataFrame API is available in the Java, Python, R, and Scala languages.
# MAGIC - **Dataset:** A combination of DataFrame and RDD. It provides the typed interface that is available in RDDs while providing the convenience of the DataFrame. The Dataset API is available in the Java and Scala languages.
# MAGIC
# MAGIC In many scenarios, especially with the performance optimizations embedded in DataFrames and Datasets, it will not be necessary to work with RDDs. But it is important to understand the RDD abstraction because:
# MAGIC - The RDD is the underlying infrastructure that allows Spark to run so fast and provide data lineage.
# MAGIC - If you are diving into more advanced components of Spark, it may be necessary to use RDDs.
# MAGIC - The visualizations within the Spark UI reference RDDs.
# MAGIC
# MAGIC #### 1.0. Apache Spark File Utilities
# MAGIC Databricks Utilities `(dbutils)` make it easy to perform powerful combinations of tasks. You can use the utilities to work with object storage efficiently, to chain and parameterize notebooks, and to work with secrets. `dbutils` are not supported outside of notebooks.
# MAGIC
# MAGIC ##### 1.1. List available commands for a utility
# MAGIC To list available commands for a utility along with a short description of each command, run `.help()` after the programmatic name for the utility. For example, the following command lists the available commands for the Databricks File System (DBFS) utility.

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. List the Contents of a Folder
# MAGIC The following command lists the contents of a folder in the Databricks File System (DBFS). In this example, all the sample datasets are being enumerated.

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3. Create a Table based on a Databricks Dataset
# MAGIC This code example demonstrates how to use SQL in the Databricks SQL query editor, or how to use Python in a notebook in Data Science & Engineering or Databricks Machine Learning, to create a table based on a Databricks dataset:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.people10m;
# MAGIC
# MAGIC CREATE TABLE default.people10m
# MAGIC   OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta');
# MAGIC   
# MAGIC SELECT * FROM default.people10m LIMIT 10;

# COMMAND ----------

spark.sql("DROP TABLE default.people10m2")
spark.sql("CREATE TABLE default.people10m2 OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')")

df = spark.sql("SELECT * FROM default.people10m2 LIMIT 10")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0. Structured Streaming
# MAGIC Sensors, IoT devices, social networks, and online transactions all generate data that needs to be monitored constantly and acted upon quickly. As a result, the need for large-scale, real-time stream processing is more evident than ever before. This tutorial module introduces Structured Streaming, the main model for handling streaming datasets in Apache Spark. In Structured Streaming, a data stream is treated as a table that is being continuously appended. This leads to a stream processing model that is very similar to a batch processing model. You express your streaming computation as a standard batch-like query as on a static table, but Spark runs it as an incremental query on the unbounded input table.
# MAGIC
# MAGIC #### 2.1. Load the Sample Data
# MAGIC The easiest way to get started with Structured Streaming is to use an example Azure Databricks dataset available in the /databricks-datasets folder accessible within the Azure Databricks workspace. Azure Databricks has sample event data as files in /databricks-datasets/structured-streaming/events/ to use to build a Structured Streaming application. First, take a look at the contents of this directory.

# COMMAND ----------

inputPath = "/databricks-datasets/structured-streaming/events/"
display(dbutils.fs.ls(inputPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2. Initialize the Stream
# MAGIC Since the sample data is just a static set of files, you can emulate a stream from them by reading one file at a time, in the chronological order in which they were created.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3. Start the Streaming Job
# MAGIC You start a streaming computation by defining a sink and starting it. In our case, to query the counts interactively, set the complete set of 1 hour counts to be in an in-memory table.

# COMMAND ----------

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4. Interactively Query the Stream
# MAGIC We can periodically query the counts aggregation:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT action
# MAGIC   , date_format(window.end, "MMM-dd HH:mm") AS time
# MAGIC   , count
# MAGIC FROM counts
# MAGIC ORDER BY time, action

# COMMAND ----------

