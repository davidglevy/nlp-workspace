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

from pyspark.sql.types import StructType, StructField, ArrayType, BinaryType, StringType
from pyspark.sql.functions import udf, col, lit, array_join

convert_to_text_udf = udf(convert_to_text, ArrayType(StringType()))
