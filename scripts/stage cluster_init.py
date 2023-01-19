# Databricks notebook source
# MAGIC %sh
# MAGIC mkdir -p /dbfs/scripts
# MAGIC cp ./cluster_init.sh /dbfs/scripts/cluster_init.sh

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir ./usr
# MAGIC /dbfs/scripts/cluster_init.sh
