# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="2.0"

# COMMAND ----------

DA.cleanup()
DA.init(create_db=False)
install_dtavod_datasets(reinstall=False)
print()
copy_source_dataset(f"{DA.working_dir_prefix}/source/dtavod/weather", f"{DA.paths.working_dir}/weather", "parquet", "weather")
DA.conclude_setup()
