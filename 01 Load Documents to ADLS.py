# Databricks notebook source
url_list = [
    {"url": "https://arxiv.org/pdf/2209.00540"},
    {"url": "https://arxiv.org/pdf/2209.00534"},
    {"url": "https://arxiv.org/pdf/2209.00417"},
    {"url": "https://arxiv.org/pdf/2209.00409"},
    {"url": "https://arxiv.org/pdf/2209.00391"},
    {"url": "https://arxiv.org/pdf/2209.00057"},
    {"url": "https://arxiv.org/pdf/2209.00197"},
    {"url": "https://arxiv.org/pdf/2209.00143"},
    {"url": "https://arxiv.org/pdf/2208.14972"},
    {"url": "https://arxiv.org/pdf/2208.14902"},
    {"url": "https://arxiv.org/pdf/2208.14653"},
    {"url": "https://arxiv.org/pdf/2208.14651"},
    {"url": "https://arxiv.org/pdf/2208.14650"},
    {"url": "https://arxiv.org/pdf/2208.14570"},
    {"url": "https://arxiv.org/pdf/2208.14560"},
    {"url": "https://arxiv.org/pdf/2208.14833"},
    {"url": "https://arxiv.org/pdf/2208.14591"},
    {"url": "https://arxiv.org/pdf/2208.14254"},
    {"url": "https://arxiv.org/pdf/2208.14248"},
    {"url": "https://arxiv.org/pdf/2208.14121"}

    
    
]

# COMMAND ----------

import re

def extract_file_name(input):
    result = re.search(".*\\/([^\\/]+)", input)
    return result.group(1)

print(extract_file_name("https://arxiv.org/pdf/2208.14121"))

# COMMAND ----------

import urllib
from urllib import request
import shutil
import os

from pathlib import Path

downloads_dir = "/tmp/downloads"
dbutils.fs.mkdirs("file://" + downloads_dir)
dbutils.fs.mkdirs("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/documents")

for download in url_list:
    file_name = extract_file_name(download['url'])

    local_file = downloads_dir + os.sep + file_name
    
    
    print(f"Downloading [{download['url']}] to [{local_file}]")
    request.urlretrieve(download['url'], local_file)
    dbutils.fs.cp("file://" + local_file, f"abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/documents/{file_name}")
    




