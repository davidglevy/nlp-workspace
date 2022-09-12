# Databricks notebook source
df = spark.table("nlp.documents.document_images").select("path", "page")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nlp.documents.document_titles;

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE nlp.documents.documents;

# COMMAND ----------

import layoutparser as lp
model = lp.Detectron2LayoutModel(
            config_path ='lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config', # In model catalog
            label_map   = {0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"}, # In model`label_map`
            extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] # Optional
        )
