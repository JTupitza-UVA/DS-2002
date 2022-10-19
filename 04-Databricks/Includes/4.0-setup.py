# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.0"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

class DataFactory:
    def __init__(self, ):
        self.source = "/mnt/training/healthcare/tracker/streaming/"
        self.userdir = f"{DA.paths.working_dir}/tracker"
        self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
            
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the tracker dataset")
            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

# COMMAND ----------

DA.init()
DA.paths.checkpoints = f"{DA.paths.working_dir}/checkpoints"    
DA.data_factory = DataFactory()

print()
DA.data_factory.load()
DA.conclude_setup()

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

