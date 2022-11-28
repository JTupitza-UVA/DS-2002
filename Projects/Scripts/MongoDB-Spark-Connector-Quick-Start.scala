// Databricks notebook source
// MAGIC %md # The MongoDB Connector for Apache Spark
// MAGIC This notebook provides a top-level technical introduction to combining Apache Spark with MongoDB, enabling developers and data engineers to bring sophisticated real-time analytics and machine learning to live, operational data.
// MAGIC 
// MAGIC The following illustrates how to use MongoDB and Spark with an example application that uses Spark's alternating least squares (ALS) implementation to generate a list of movie recommendations for a user. This notebook covers:
// MAGIC 1. How to read data from MongoDB into Spark. 
// MAGIC 2. How to run the MongoDB Connector for Spark as a library in Databricks.
// MAGIC 3. How to use the machine learning ALS library in Spark to generate a set of personalized movie recommendations for a given user.
// MAGIC 4. How to write the recommendations back to MongoDB so they are accessible to applications.
// MAGIC 
// MAGIC ## Create Databricks Cluster and Add the Connector as a Library
// MAGIC 
// MAGIC 1. Create a Databricks cluster.
// MAGIC 1. Navigate to the cluster detail page and select the **Libraries** tab.
// MAGIC 1. Click the **Install New** button.
// MAGIC 1. Select **Maven** as the Library Source.
// MAGIC 1. Enter the Mongo DB Connector for Spark package value into the **Coordinates** field based on your Databricks Runtime version:
// MAGIC    1. For Databricks Runtime 7.0.0 and above, enter `org.mongodb.spark:mongo-spark-connector_2.12:3.0.0`.
// MAGIC    1. For Databricks Runtime 5.5 LTS and 6.x, enter `org.mongodb.spark:mongo-spark-connector_2.11:2.3.4`.
// MAGIC 1. Click **Install**.
// MAGIC 
// MAGIC ## Prepare a MongoDB Atlas Instance
// MAGIC 
// MAGIC Atlas is a fully managed, cloud-based MongoDB service. We'll use Atlas to test the integration between MongoDb and Spark.
// MAGIC 
// MAGIC 1. Sign up for [MongoDB Atlas](https://www.mongodb.com/cloud/atlas?jmp=docs). 
// MAGIC 1. [Create an Atlas free tier cluster](https://docs.atlas.mongodb.com/getting-started/).
// MAGIC 1. Enable Databricks clusters to connect to the cluster by adding the external IP addresses for the Databricks cluster nodes to the [whitelist in Atlas](https://docs.atlas.mongodb.com/setup-cluster-security/#add-ip-addresses-to-the-whitelist). 
// MAGIC 
// MAGIC ## Import MovieLens Data into Atlas
// MAGIC 
// MAGIC This example uses the [MovieLens](https://grouplens.org/datasets/movielens/) dataset. Download the [small MovieLens dataset (ml-latest-small.zip)](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip) and import it into MongoDB with ``mongoimport``. You can find instructions on installing and using ``mongoimport`` in the [The MongoDB Database Tools Documentation](https://docs.mongodb.com/database-tools/). For the MovieLens dataset, the command will look like:
// MAGIC 
// MAGIC ```
// MAGIC mongoimport --host <cluster-id> --ssl --username <user> --password <password> --authenticationDatabase admin --db recommendation --collection ratings --type CSV --file ml-latest-small/ratings.csv --headerline
// MAGIC ```
// MAGIC 
// MAGIC ## Configure Databricks Cluster with MongoDB Connection URI
// MAGIC 
// MAGIC 1. Get the MongoDB connection URI. In the MongoDB Atlas UI, click the cluster you created.
// MAGIC 
// MAGIC    1. Click the **Connect** button.
// MAGIC    1. Click **Connect Your Application**.
// MAGIC    1. Select **Scala** in the **Driver** dropdown and **2.2 or later** in the version dropdown.
// MAGIC    1. Copy the generated connection string. It should look like `mongodb+srv://<user>:<password>@<cluster-name>-wlcof.azure.mongodb.net/test?retryWrites=true`
// MAGIC    1. Configure the user, password, and cluster-name values.
// MAGIC 1. In the cluster detail page for your Databricks cluster, select the **Configuration** tab.
// MAGIC 1. Click the **Edit** button.
// MAGIC 1. Under **Advanced Options**, select the **Spark** configuration tab and update the **Spark Config** using the connection string you copied in the previous step:
// MAGIC 
// MAGIC     ```
// MAGIC     spark.mongodb.output.uri <connection-string>
// MAGIC     spark.mongodb.input.uri <connection-string>
// MAGIC     ```
// MAGIC 
// MAGIC ## Read Data From MongoDB
// MAGIC   
// MAGIC The Spark Connector can be configured to read from MongoDB in a number of ways, each of which is detailed in the [MongoDB docs](https://docs.mongodb.com/spark-connector/current/configuration/). This example uses the SparkSesssion object directly, via an options map. The SparkSession reads from the "ratings" collection in the "recommendation" database.

// COMMAND ----------

import com.mongodb.spark._

val ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "recommendation").option("collection", "ratings").load()

// COMMAND ----------

// MAGIC %md One of the most attractive features of MongoDB is support for flexible schemas, which enables you to store a variety of JSON models in the same collection. A consequence of flexible schemas is that there is no defined schema for a given collection as there would be in an RDBMS. Since DataFrames and Datasets require a schema, the Spark Connector will automatically infer the schema by randomly [sampling documents](https://docs.mongodb.com/manual/reference/operator/aggregation/sample/) from the database. It then assigns that inferred schema to the DataFrame. 
// MAGIC 
// MAGIC The Spark Connector's ability to infer schema through document sampling is a nice convenience, but if you know your document structure you can assign the schema explicitly and avoid the need for sampling queries. The following example shows you how to define a DataFrame's schema explicitly by using a case class.

// COMMAND ----------

case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

import spark.implicits._
val ratingsDS = ratings.as[Rating]
ratingsDS.cache()
ratingsDS.show()

// COMMAND ----------

// MAGIC %md ## Using Machine Learning Libraries 
// MAGIC In this example, we use the ALS library for Apache Spark to learn our dataset in order to make predictions for a user. This example is detailed as a five step process. You can learn more about how ALS generates predictions in the [Spark documentation](https://spark.apache.org/docs/2.3.0/ml-collaborative-filtering.html).
// MAGIC 
// MAGIC ### Creating the Machine Learning Model
// MAGIC For training purposes, the complete data set must also split into smaller partitions known as the training, validation, and test data. In this case, 80% of the data will be used for training and the rest can be used to validate the model.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

/* import org.apache.spark.ml.recommendation.ALS.Rating */
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
val model = als.fit(training)


// COMMAND ----------

// MAGIC %md Here we evaluate the model by computing the RMSE on the test data.

// COMMAND ----------

// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")
val predictions = model.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")


// COMMAND ----------

// MAGIC %md Finally, we use the model to generate a set of 10 recommended movies for each user in the dataset, and write those recommendations back to MongoDB.

// COMMAND ----------

// Write recommendations back to MongoDB
import org.apache.spark.sql.functions._
val docs  = predictions.map( r => ( r.getInt(4), r.getInt(1),  r.getDouble(2) ) ).toDF( "userID", "movieId", "rating" )
docs.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("database", "recommendation").option("collection", "recommendations").save()
