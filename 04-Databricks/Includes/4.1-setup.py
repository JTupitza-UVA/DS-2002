# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.1"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .load(data_source)
        .writeStream
        .option("checkpointLocation", checkpoint_directory)
        .option("mergeSchema", "true")
        .trigger(once=True)
        .table(table_name)
        .awaitTermination()
    )

# COMMAND ----------

class DataFactory:
    def __init__(self):
        self.source = "/mnt/training/healthcare/tracker/streaming/"
        self.userdir = f"{DA.paths.working_dir}/tracker"
        self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
            
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                dbutils.fs.cp(f"{self.source}/{curr_file}", f"{self.userdir}/{curr_file}")
                self.curr_mo += 1
                autoload_to_table(self.userdir, "json", "bronze", f"{DA.paths.checkpoints}/bronze")
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")
            
            dbutils.fs.cp(f"{self.source}/{curr_file}", f"{self.userdir}/{curr_file}")
            self.curr_mo += 1
            autoload_to_table(self.userdir, "json", "bronze", f"{DA.paths.checkpoints}/bronze")
        

# COMMAND ----------

DA.init()
DA.paths.checkpoints = f"{DA.paths.working_dir}/_checkpoints"    
DA.data_factory = DataFactory()

print()
DA.data_factory.load()
DA.conclude_setup()

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

