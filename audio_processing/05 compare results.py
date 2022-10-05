# Databricks notebook source
# MAGIC %pip install nltk

# COMMAND ----------

with open('./obama_inaugural_address.txt', encoding="utf-8") as f:
    data = f.read()

print(data[0:20])

df_expected = spark.createDataFrame([{'speech_text': data}])

# COMMAND ----------

import nltk
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

nltk.download('stopwords')
stop_words = stopwords.words('english')

def is_stop_word(input):
    if (input is None):
        return True
    return (input and input in stop_words)

print("is 'the' a stop word: " + str(is_stop_word("the")))
print("is 'dave' a stop word: " + str(is_stop_word("dave")))
print("is None a stop word: " + str(is_stop_word(None)))

is_stop_word_udf = udf(is_stop_word, BooleanType())
spark.udf.register("is_stop_word", is_stop_word, BooleanType())

# COMMAND ----------

from pyspark.sql.functions import split, col, explode

df_processed = df_expected.select(explode(split(col("speech_text"), "[^a-zA-Z\d:]")).alias("word"))

df_processed.createOrReplaceTempView("expected_obama_words")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nlp.audio.speech_reference_counts AS
# MAGIC SELECT 'obama' AS who, word, count(word) as word_count
# MAGIC FROM expected_obama_words
# MAGIC WHERE word is not null and word regexp '[a-zA-Z]+' and not is_stop_word(lower(word))
# MAGIC GROUP BY word
# MAGIC ORDER BY word_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nlp.audio.speech_reference_counts ORDER BY word_count DESC;
