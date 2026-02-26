# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.5L"

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)
DA.conclude_setup()

