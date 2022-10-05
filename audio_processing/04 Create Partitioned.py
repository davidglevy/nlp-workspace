# Databricks notebook source
df = spark.table("nlp.audio.audio_converted_v2")

# COMMAND ----------

from pyspark.sql.functions import StringType, udf, col

def create_partition_hash(input):
    return str(hash(input))[0:2]

create_partition_hash_udf = udf(create_partition_hash, StringType())

# COMMAND ----------

df_partitioned = df.withColumn("partition", create_partition_hash_udf(col("site")))
df_partitioned.write.format("delta").partitionBy("partition").mode("overwrite").saveAsTable("nlp.audio.audio_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL nlp.audio.audio_partitioned
