# Databricks notebook source
import numpy as np
import layoutparser as lp
import io
from PIL import Image



# COMMAND ----------

# MAGIC %r
# MAGIC dbutils.fs.ls("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/documents")

# COMMAND ----------

image_indexes = spark.sql("SELECT path, page FROM nlp.documents.document_images").collect()

# COMMAND ----------

# Load layout parser model
model = lp.Detectron2LayoutModel(
        config_path ='lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config', # In model catalog
        label_map   = {0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"}, # In model`label_map`
        extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] # Optional
     )
# Load the OCR Agent
ocr_agent = lp.TesseractAgent(languages='eng')


def extractTitles(input, model, ocr_agent):
    # Convert back into image
    stream = io.BytesIO(input) 
    image = Image.open(stream)
    
    
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


# COMMAND ----------

results = []

for row in image_indexes:
    path = row['path']
    page = row['page']
    print(f"Processing path [{path}] on page [{page}]")
    image_binary = spark.sql(f"SELECT image FROM nlp.documents.document_images WHERE path = '{path}' AND page = {page}").collect()
    
    titles = extractTitles(image_binary[0]['image'], model, ocr_agent)
    
    print(f"Found: {titles}")
    
    result = {
        'path' : path,
        'page' : page,
        'titles' : titles
    }
    
    results.append(result)

          

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

schema = StructType([
    StructField("path", StringType(), True),
    StructField("page", IntegerType(), True),
    StructField("titles", ArrayType(StringType()), True)
])

# COMMAND ----------

result_df = spark.createDataFrame(results, schema)


# COMMAND ----------

from pyspark.sql.functions import posexplode, col

df_titles = result_df.select("path", "page", posexplode(col("titles")).alias("title_index_in_page", "title"))

#df_titles.write.format("delta").option("overwrite", True).saveAsTable("nlp.documents.document_titles")

# COMMAND ----------

df_titles.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("nlp.documents.document_titles")

# COMMAND ----------

display(df_titles)
