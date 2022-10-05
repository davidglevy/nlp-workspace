# Databricks notebook source
import azure.cognitiveservices.speech as speechsdk
from azure.cognitiveservices.speech import SpeechRecognizer, SpeechConfig
from pydub import AudioSegment
import tempfile
#from urllib import request
from io import BytesIO
#import requests
import json
import time

# COMMAND ----------

dbutils.widgets.dropdown("Testing", 'No', ['Yes', 'No'])

# COMMAND ----------

def convert_to_text(input, key):
    
    # TODO Add type conversion to pipeline.
    sound_file = AudioSegment.from_mp3(BytesIO(input))

    exported = BytesIO()
    sound_file.export(exported, "wav")
    converted = exported.read()

    
    
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, 'wb') as f:
            f.write(converted)
        
        speech_config = speechsdk.SpeechConfig(subscription=key, region="AustraliaEast")
        audio_input = speechsdk.AudioConfig(filename=tmp.name)
        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input)

    done = False

    all_results = []

    def handle_final_result(evt):
        if (evt.result.reason == speechsdk.ResultReason.RecognizedSpeech):
            text = evt.result.text
            print(f"RECOGNIZED: {text}")
            all_results.append(evt.result.text)
        else:
            print("Ignoring non-success result {}".format(evt))
    
    def stop_cb(evt):
        """callback that stops continuous recognition upon receiving an event `evt`"""
        print('CLOSING on {}'.format(evt))
        speech_recognizer.stop_continuous_recognition()
        nonlocal done
        done = True

    # Connect callbacks to the events fired by the speech recognizer
    # Commented this out - noisy
    #speech_recognizer.recognizing.connect(lambda evt: print('RECOGNIZING: {}'.format(evt)))
    speech_recognizer.recognized.connect(handle_final_result)
    speech_recognizer.session_started.connect(lambda evt: print('SESSION STARTED: {}'.format(evt)))
    speech_recognizer.session_stopped.connect(lambda evt: print('SESSION STOPPED {}'.format(evt)))
    speech_recognizer.canceled.connect(lambda evt: print('CANCELED {}'.format(evt)))
    # stop continuous recognition on either session stopped or canceled events
    speech_recognizer.session_stopped.connect(stop_cb)
    speech_recognizer.canceled.connect(stop_cb)

    # Start continuous speech recognition
    speech_recognizer.start_continuous_recognition()
    while not done:
        time.sleep(.5)
        
    return all_results

    



# COMMAND ----------

# Get SaaS Key
key = dbutils.secrets.get("saas_keys", "azure_speech")
print(f"The key is [{key}]")
    
#token = get_token(key)

testing = dbutils.widgets.get("Testing")

if (testing == 'Yes'):
    df = spark.table("nlp.audio.audio_raw").limit(1)

    payload = df.select("content").take(1)[0]['content']

    all_results = convert_to_text(payload, key)

    for result in all_results:
        print(f"Converted audio part into: {result}")

    

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, BinaryType, StringType
from pyspark.sql.functions import udf, col, lit, array_join

audio_fragment_schema = ArrayType(BinaryType())
convert_to_text_udf = udf(convert_to_text, ArrayType(StringType()))



# COMMAND ----------

#TODO If we want to keep the "parts" before joining back together, re-instate this from the v1 version.

df_raw = spark.table("nlp.audio.audio_raw")

df_translated = df_raw.withColumn("speech_as_text", array_join(convert_to_text_udf(col("content"), lit(key)), " "))
                 
df_translated.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("nlp.audio.audio_converted_v2")


# COMMAND ----------

df_final = spark.table("nlp.audio.audio_converted")
display(df_final)
