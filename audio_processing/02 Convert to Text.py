# Databricks notebook source
# MAGIC %sh
# MAGIC apt-get install -y ffmpeg

# COMMAND ----------

# MAGIC %pip install pydub

# COMMAND ----------

df = spark.table("nlp.documents.audio_raw").limit(1)

# COMMAND ----------

from pydub import AudioSegment
from pydub.silence import split_on_silence


# COMMAND ----------

from urllib import request
from io import BytesIO
import requests

from pydub import AudioSegment
from pydub.silence import split_on_silence

threshold = 60 * 1000

def exportAudioSegment(input):
    exported = BytesIO()
    input.export(exported, "wav")
    return exported.read()

def split_into_chunks(input):
    print(type(input))
    sound_file = AudioSegment.from_mp3(BytesIO(input))
    audio_chunks = split_on_silence(sound_file, min_silence_len=500, silence_thresh=-16,keep_silence=200)

    results = []
    first = True
    current = None
    for audio_chunk in audio_chunks:
        if first:
            current = audio_chunk
            first = False
        else:
            temp = current + audio_chunk
            if (len(temp) >= threshold):
                # Too big, save existing chunk and start a new one.
                results.append(exportAudioSegment(current))
                current = audio_chunk
            else:
                current = temp
    if (current):
        results.append(exportAudioSegment(current))
    return results

def get_token(subscription_key):
    fetch_token_url = 'https://australiaeast.api.cognitive.microsoft.com/sts/v1.0/issueToken'
    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key
    }
    response = requests.post(fetch_token_url, headers=headers)
    access_token = str(response.text)
    return access_token


def convert_to_text(input, key):
    url = "https://australiaeast.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US"
    #url = "https://australiaeast.api.cognitive.microsoft.com/sts/v1.0/issuetoken"
    
    headers = { 
        "Content-type": 'audio/wav;codec="audio/pcm";',
        'Ocp-Apim-Subscription-Key': key
    }

    
    response = requests.request("POST", url, headers=headers, data=input)
    print(response)
    return response.text
    
# Get SaaS Key
key = dbutils.secrets.get("saas_keys", "azure_speech")
print(f"The key is [{key}]")
    
token = get_token(key)

payload = df.select("content").take(1)[0]['content']
print(type(payload))

audio_parts = split_into_chunks(payload)

x = 0
for audio_part in audio_parts:
    result = convert_to_text(audio_part, key)
    print(f"Converted audio part [{x}] into: {result}")
    x = x + 1
    break;
    


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, BinaryType, StringType
from pyspark.sql.functions import udf, col, posexplode, lit

audio_fragment_schema = ArrayType(BinaryType())

split_into_chunks_udf = udf(split_into_chunks, audio_fragment_schema)
convert_to_text_udf = udf(convert_to_text, StringType())

df_raw = spark.table("nlp.documents.audio_raw")
df_chunks = df_raw.withColumn("audio_chunks", split_into_chunks_udf(col("content"))).select("url", posexplode(col("audio_chunks")).alias("chunk_index", "audio_chunk"))
df_chunks.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("nlp.documents.audio_raw_chunks")


#df_translated = df_chunks.withColumn("speech_as_text", convert_to_text_udf(col("audio_chunk"), lit(key)))

#df_translated.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("nlp.documents.audio_converted_parts")

