# Databricks notebook source
df = spark.read.format("binaryFile").load("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/documents")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS nlp;
# MAGIC USE CATALOG nlp;
# MAGIC CREATE DATABASE documents;
# MAGIC USE documents;

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwrite", True).saveAsTable("nlp.documents.downloads")
