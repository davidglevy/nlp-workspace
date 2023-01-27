# Databricks notebook source
spark.sql("USE nlp.documents")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE expanded_content;

# COMMAND ----------

# MAGIC %pip install transformers sentence-transformers

# COMMAND ----------

import torch
from transformers import BertTokenizer, BertModel, pipeline


import logging
import matplotlib.pyplot as plt 
%matplotlib inline

tokenizer = BertTokenizer.from_pretrained('bert-large-uncased')
model = BertModel.from_pretrained('bert-large-uncased')

pipe = pipeline(model='bert-large-uncased', tokenizer=tokenizer, device=0)

# COMMAND ----------

from pyspark.sql.functions import udf, StringType

device = "cuda:0" if torch.cuda.is_available() else "cpu"

def create_input(text):
    return '[CLS] ' + text + '[SEP]'

create_input_udf = udf(create_input, StringType())

df = spark.table('expanded_content').withColumn('bert_input_text', create_input_udf('text'))

display(df)

# COMMAND ----------

def tokenize_text(input_text):
    return tokenizer.tokenize(input_text)

text_rows = df.select("bert_input_text").collect()
text = [ row["bert_input_text"] for row in text_rows ]



# COMMAND ----------

from sentence_transformers import SentenceTransformer
model = SentenceTransformer('bert-base-nli-mean-tokens')
#Encoding:
sen_embeddings = model.encode(text)
sen_embeddings.shape

# COMMAND ----------

from sklearn.metrics.pairwise import cosine_similarity
#let's calculate cosine similarity for sentence 0:
cosine_similarity(
    [sen_embeddings[0]],
    sen_embeddings[1:]
)

# COMMAND ----------


