# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="1.0"

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)
DA.conclude_setup()
