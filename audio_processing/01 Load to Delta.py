# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS nlp;
# MAGIC CREATE DATABASE IF NOT EXISTS nlp.audio;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, BinaryType
from urllib import request

url_list = [
    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamafederalplaza.mp3", "title": "Federal Plaza Address Opposing the War in Iraq", "date": "02 Oct 2002"}
#    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobama2004dncARXE.mp3", "title": "Democratic National Convention Keynote Speech", "date": "27 Jul 2004"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamasenatespeechohiovotecounting.mp3", "title": "Senate Speech on Ohio Electoral Vote", "date": "06 Jan 2005"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamaknoxcommencement.mp3", "title": "Knox College Commencement Speech", "date": "04 Jun 2005"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamasenatespeechrosaparks.mp3", "title": "Senate Speech Honoring the Life of Rosa Parks", "date": "25 Oct 2005"},
#    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobamasenatefloorspeechpatriotactARXE.mp3", "title": "Senate Speech on the PATRIOT Act", "date": "15 Dec 2005"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamasenatespeechcorettascottking.mp3", "title": "Senate Speech Honoring the Life of Coretta Scott King", "date": "31 Jan 2006"},
#    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobamasenatespeechvotingrightsactARXE.mp3", "title": "Senate Speech on Voting Rights Act Renewal", "date": "20 Jul 2006"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamaexploratorycommittee.mp3", "title": "Presidential Exploratory Committee Announcement", "date": "16 Jan 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamacandidacyannouncement.mp3", "title": "Presidential Candidacy Announcement", "date": "10 Feb 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamabrownchapel.mp3", "title": "Brown Chapel Speech", "date": "04 Mar 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamasenatespeechiraqfederalism.mp3", "title": "Senate Speech on Iraq Federalism Amendment", "date": "13 Mar 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamasenatespeechiraqwar4years.mp3", "title": "Senate Speech on Iraq War After 4 Years", "date": "21 Mar 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamasenatespeechimmigrationreform.mp3", "title": "Senate Speech on Comprehensive Immigration Reform", "date": "23 May 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/barackobama/barackobamawilsoncenter.mp3", "title": "Woodrow Wilson Center Speech", "date": "01 Aug 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobamajeffersonjacksondinnerARXE.mp3", "title": "Speech at the Jefferson-Jackson Dinner", "date": "10 Nov 2007"},
#    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobamaiowacaucusspeechvictoryARXE.mp3", "title": "Iowa Caucus Victory Speech", "date": "03 Jan 2008"}#,
#    {"url": "https://www.americanrhetoric.com/mp3clipsXE/barackobama/barackobamainauguraladdressARXE.mp3"},
#    {"url": "https://www.americanrhetoric.com/mp3clips/politicalspeeches/jfkinauguralsurround.mp3"}
]

schema_load = StructType([
    StructField("title", StringType(), False),
    StructField("url", StringType(), False),
    StructField("date", StringType(), False),
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

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nlp.audio.audio_raw;
