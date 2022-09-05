# Databricks notebook source


# COMMAND ----------

df = spark.table("nlp.documents.audio_raw").limit(1)

# COMMAND ----------

from urllib import request
import requests

def get_token(subscription_key):
    fetch_token_url = 'https://australiaeast.api.cognitive.microsoft.com/sts/v1.0/issueToken'
    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key
    }
    response = requests.post(fetch_token_url, headers=headers)
    access_token = str(response.text)
    return access_token


def convert_to_text(input, key):
    #url = "https://australiaeast.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US"
    url = "https://australiaeast.api.cognitive.microsoft.com/sts/v1.0/issuetoken"
    
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

result = convert_to_text(payload, key)
print(result)
