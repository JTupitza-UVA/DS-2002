# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="5.0"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

class DataFactory:
    def __init__(self):
        self.source = f"{DA.paths.data_source}/tracker/streaming/"
        self.userdir = DA.paths.data_landing_location
        self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.userdir}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1


# COMMAND ----------

DA.init()
DA.paths.checkpoints = f"{DA.paths.working_dir}/_checkpoints"    
DA.paths.data_source = "/mnt/training/healthcare"
DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

# bronzePath             = f"{DA.paths.wokring_dir}/bronze"
# recordingsParsedPath   = f"{DA.paths.wokring_dir}/silver/recordings_parsed"
# recordingsEnrichedPath = f"{DA.paths.wokring_dir}/silver/recordings_enriched"
# dailyAvgPath           = f"{DA.paths.wokring_dir}/gold/dailyAvg"

# checkpointPath               = f"{DA.paths.wokring_dir}/checkpoints"
#bronzeCheckpoint             = f"{DA.paths.checkpoints}/bronze"
# recordingsParsedCheckpoint   = f"{DA.paths.checkpoints}/recordings_parsed"
# recordingsEnrichedCheckpoint = f"{DA.paths.checkpoints}/recordings_enriched"
# dailyAvgCheckpoint           = f"{DA.paths.checkpoints}/dailyAvgPath"

DA.data_factory = DataFactory()
DA.conclude_setup()

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

