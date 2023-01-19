# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE reconstituted
# MAGIC AS
# MAGIC (SELECT *
# MAGIC FROM nlp.audio.audio_converted);

# COMMAND ----------

from urllib import request

request.urlretrieve("https://www.dropbox.com/s/57zjbwv6gh3srry/model_final.pth?dl=1", "/tmp/model_final.pth")

# COMMAND ----------

request.urlretrieve("https://www.dropbox.com/s/nau5ut6zgthunil/config.yaml?dl=1", "/tmp/config_tmp.yaml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lets remove the remote weights model to our local one

# COMMAND ----------

with open("/tmp/config_tmp.yaml", "rt") as fin:
    with open("/tmp/config.yaml", "wt") as fout:
        for line in fin:
            if ("  WEIGHTS:" in line):
                fout.write("  WEIGHTS: /dbfs/fwc/layoutparser/model_final.pth\n")
            else:
                fout.write(line)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy Layout Parser files to DBFS

# COMMAND ----------

dbutils.fs.mkdirs("/fwc/layoutparser")
dbutils.fs.cp("file:///tmp/model_final.pth", "/fwc/layoutparser/model_final.pth")
dbutils.fs.cp("file:///tmp/config.yaml", "/fwc/layoutparser/config.yaml")
