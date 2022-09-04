# Databricks notebook source
url_list = [
    {"url": "https://arxiv.org/pdf/2209.00540", "name": "2209.00540.pdf"},
    {"url": "https://arxiv.org/pdf/2209.00534", "name": "2209.00534.pdf"},
    {"url": "https://arxiv.org/pdf/2209.00417", "name": "2209.00417.pdf"}
]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("url", StringType(), True)
    StructField("name", StringType(), True)
])


df = spark.createDataFrame(url_list, schema=schema)

# COMMAND ----------

import urllib
from urllib import request
import shutil
import os

dbutils.fs.mkdirs("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/documents")
dbutils.fs.mkdirs("file:///tmp/downloader")

# Download URL and save to outpath.
def downloader(url, file_name, outpath):
    # From URL construct the destination path and filename.
    #file_name = os.path.basename(urllib.parse.urlparse(url).path)
    file_path = os.path.join("/tmp/downloader", file_name)
    
    # Download and write to file.
    with urllib.request.urlopen(url, timeout=5) as urldata, \
      open(file_path, 'wb') as out_file:
        shutil.copyfileobj(urldata, out_file)
    
    # Copy the temporary file to the outpath
    storePath = f"{outpath}/{file_name}"
    dbutils.fs.cp(f"file:///{file_path}", storePath)
        



# COMMAND ----------

downloader("https://arxiv.org/pdf/2209.00417", "2209.00417.pdf", "abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/documents")
