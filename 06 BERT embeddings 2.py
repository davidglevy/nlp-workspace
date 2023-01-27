# Databricks notebook source
# MAGIC %pip install transformers sentence-transformers

# COMMAND ----------

spark.sql("USE nlp.documents")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE expanded_content;

# COMMAND ----------

# Example from here: https://huggingface.co/course/chapter5/6?fw=tf
from transformers import AutoTokenizer, TFAutoModel

#model_ckpt = "sentence-transformers/multi-qa-mpnet-base-dot-v1"
#tokenizer = AutoTokenizer.from_pretrained(model_ckpt)
#model = TFAutoModel.from_pretrained(model_ckpt, from_pt=True)

# COMMAND ----------

def cls_pooling(model_output):
    return model_output.last_hidden_state[:, 0]

# COMMAND ----------

def get_embeddings(text_list):
    encoded_input = tokenizer(
        text_list, padding=True, truncation=True, return_tensors="tf"
    )
    encoded_input = {k: v for k, v in encoded_input.items()}
    model_output = model(**encoded_input)
    return cls_pooling(model_output)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lets check functionality locally

# COMMAND ----------

text = spark.table("expanded_content").select("text").limit(1).collect()[0]["text"]

# COMMAND ----------

embedding = get_embeddings(text)
embedding.shape

# COMMAND ----------

all_text_rows = spark.table("expanded_content").select("text").collect()
all_text = [row["text"] for row in all_text_rows]

embeddings = []
for text in all_text:
    embedding = get_embeddings(text).numpy()[0]
    embeddings.append(embedding)




# COMMAND ----------

print(embeddings[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now Create Pandas UDF for embedding creation.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
import json

def extractTitlesPd(inputs: pd.Series) -> pd.Series:

    model_ckpt = "sentence-transformers/multi-qa-mpnet-base-dot-v1"
    tokenizer = AutoTokenizer.from_pretrained(model_ckpt)
    model = TFAutoModel.from_pretrained(model_ckpt, from_pt=True)

    inputs_list = inputs.tolist()

    results = []

    for text_list in inputs_list:
        encoded_input = tokenizer(
            text_list, padding=True, truncation=True, return_tensors="tf"
        )
        encoded_input = {k: v for k, v in encoded_input.items()}
        model_output = model(**encoded_input)
        result_text = cls_pooling(model_output).numpy()[0]

        results.append(result_text)
    
    return pd.Series(results)

# COMMAND ----------

#series = pd.Series(all_text)
#result_pd = extractTitlesPd(series)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType

extractTitlesPd_udf = pandas_udf(extractTitlesPd, ArrayType(DoubleType()))

result_df = spark.table("expanded_content").repartition(1).limit(20).withColumn("text_bert_embeddings", extractTitlesPd_udf("text"))
display(result_df)

# COMMAND ----------

result_df = result_df.select("path", "page", "section_index", "continuation", "text_index", "text_bert_embeddings")
result_df.write.format("delta").mode("overwrite").saveAsTable("text_bert_embeddings")
