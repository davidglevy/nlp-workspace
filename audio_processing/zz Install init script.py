# Databricks notebook source
# MAGIC %sh
# MAGIC ls -la .

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /dbfs/scripts
# MAGIC cp ./audio_init_script.sh  /dbfs/scripts/audio_cluster_init.sh

# COMMAND ----------

cat /dbfs/scripts/audio_cluster_init.sh
