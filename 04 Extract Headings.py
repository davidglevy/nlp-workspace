# Databricks notebook source
import numpy as np
import layoutparser as lp
import io
from PIL import Image



# COMMAND ----------

taken = spark.table("nlp.documents.document_images").select("image").take(1)
page = taken[0]["image"]

# COMMAND ----------

# TODO Shift this into a Pandas UDF function.

def extractTitles(input):
    # Convert back into image
    stream = io.BytesIO(input) 
    image = Image.open(stream)
    
    # Load layout parser model
    model = lp.Detectron2LayoutModel(
            config_path ='lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config', # In model catalog
            label_map   = {0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"}, # In model`label_map`
            extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] # Optional
        )
    # Load the OCR Agent
    ocr_agent = lp.TesseractAgent(languages='eng')
    
    # Process
    image_np = np.array(image)
    layout = model.detect(image_np)
    
    text_blocks = lp.Layout([b for b in layout if b.type == 'Title'])
    
    for block in text_blocks:
        segment_image = (block
                        .pad(left=5, right=5, top=5, bottom=5)
                        .crop_image(image_np))
        text = ocr_agent.detect(segment_image)
        block.set(text=text, inplace=True)
    
    result = []
    for i, txt in enumerate(text_blocks.get_texts()):
        stripped = txt.strip()
        if (stripped):
            result.append(stripped)
    
    return result

#result = extractTitles(page)

# COMMAND ----------

#print(result)

# COMMAND ----------

from pyspark.sql.functions import udf, col, posexplode
from pyspark.sql.types import ArrayType, StringType

schema = ArrayType(StringType())

extractTitles_udf = udf(extractTitles, schema)

# COMMAND ----------

df = spark.table("nlp.documents.document_images").limit(1)

# COMMAND ----------


df_titles = df.select("path","page", posexplode(extractTitles_udf(col("image"))).alias("title_index_in_page", "title"))

# COMMAND ----------

df_titles.write.format("delta").option("overwrite", True).saveAsTable("nlp.documents.document_titles")

# COMMAND ----------

df = spark.table("nlp.documents.document_images").limit(1)
df_titles = df.withColumn("titles", extractTitles_udf(col("image")))

# COMMAND ----------

display(df_titles)

# COMMAND ----------

#%sh
#add-apt-repository ppa:alex-p/tesseract-ocr-devel

# COMMAND ----------

#%sh
#apt install -y tesseract-ocr
