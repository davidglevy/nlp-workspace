# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG nlp;
# MAGIC CREATE DATABASE nlp.audio;

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, BinaryType
from urllib import request

url_list = [
    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobamainauguraladdressARXE.mp3"},
    {"url": "https://www.americanrhetoric.com/mp3clips/politicalspeeches/jfkinauguralsurround.mp3"}
]

schema_load = StructType([
    StructField("url", StringType(), False),
    StructField("file_name", StringType(), True)
])

url_list_df = spark.createDataFrame(url_list, schema_load)

# COMMAND ----------

import re
from urllib.parse import urlparse
from urllib import parse
from urllib import request

def extract_file_name(input):
    result = re.search(".*\\/([^\\/?]+)", input)
    return result.group(1)

print(extract_file_name("https://www.americanrhetoric.com/mp3clips/politicalspeeches/jfkinaugural2.mp3?abc=123&dfg=456"))


def download_audio(input, file_name):
    
    parsed = urlparse(input)
    path = parsed.path
    if (file_name):
        print("File name provided")
    else:
        print("Extracting file name")
        file_name = extract_file_name(input)
    url = input
    site = parsed.netloc
    scheme = parsed.scheme
    query = parse.parse_qs(parsed.query)
    params = parsed.params
    
    response = request.urlopen(url)
    data = response.read()   
    
    
    result = {
        'path' : path,
        'file_name' : file_name,
        'url' : url,
        'site' : site,
        'scheme' : scheme,
        'query' : query,
# Comment out until I have an example with them non-null
        #        'params' : params,
        'content' : data
    }
    return result

#for x in url_list:
#    if 'file_name' in x:
#        result = download_audio(x['url'], x['file_name'])
#        print(len(result['content']))
#    else:
#        result = download_audio(x['url'], None)
#        print(len(result['content']))

#download = download_audio("https://www.americanrhetoric.com/mp3clips/politicalspeeches/jfkinaugural2.mp3?abc=123&dfg=456")

# COMMAND ----------

from pyspark.sql.functions import udf, col

schema = StructType([
    StructField("path", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("url", StringType(), False),
    StructField("site", StringType(), True),
    StructField("scheme", StringType(), True),
    StructField("query", MapType(StringType(), ArrayType(StringType())), True),
#    StructField("params", StringType(), True),
    StructField("content", BinaryType(), True),
])

download_audio_udf = udf(download_audio, schema)

# COMMAND ----------

downloaded_df = url_list_df.withColumn("downloaded", download_audio_udf(col("url"), col("file_name")))

# COMMAND ----------

final_df = downloaded_df.select("downloaded.path", "downloaded.file_name", "downloaded.url", "downloaded.site", "downloaded.scheme", "downloaded.query", "downloaded.content")

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("nlp.audio.audio_raw")
