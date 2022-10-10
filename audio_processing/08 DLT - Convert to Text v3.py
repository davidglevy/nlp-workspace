# Databricks notebook source
# MAGIC %pip install azure-cognitiveservices-speech pydub

# COMMAND ----------

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
        
    return input, " ".join(all_results)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import pandas as pd
from pandas import Series

def convert_to_text_temp(url, key):
    time.sleep(5)
    return url, "converted the text"

# Lets test that we can operate on each element for one invocation.
@pandas_udf('string', PandasUDFType.SCALAR)
def pandas_convert_to_text(v: Series, k: Series) -> Series:
    # TODO Ensure we have a "catch all".
    
    # Initialize a new ThreadPoolExecutor.
    # For IO Bound workloads, there can be many many more threads than cores, but for this we'll limit it to 10.
    
    # Get all the data passed to this function.
    urls = v.to_list()
    keys = k.to_list()
    
    results_dict = {}
    
    n_threads = 20
    
    with ThreadPoolExecutor(n_threads) as executor:
        futures = [executor.submit(convert_to_text, url, key) for url, key in zip(urls, keys)]

        for future in as_completed(futures):
                    # get the downloaded url data
                    url, result = future.result()
                    results_dict[url] = result
    results = []
    for url in urls:
        results.append(results_dict[url])
    
    return pd.Series(results)

# COMMAND ----------

import dlt

# Get SaaS Key
key = dbutils.secrets.get("saas_keys", "azure_speech")
print(f"The key is [{key}]")


    

# COMMAND ----------

#TODO If we want to keep the "parts" before joining back together, re-instate this from the v1 version.
from pyspark.sql.functions import array_join, lit, col

@dlt.table
def nlp_converted():
    df_raw = dlt.read("audio_raw")
    df_translated = df_raw.withColumn("speech_as_text", pandas_convert_to_text(col("content"), lit(key)))
    return df_translated
