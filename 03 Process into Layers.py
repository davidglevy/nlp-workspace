# Databricks notebook source
from pdf2image import convert_from_bytes
import cv2
import tempfile
from pyspark.sql.types import ArrayType, StructType, StructField, BinaryType
from pyspark.sql.functions import col, lit, udf
import io

#img = Image.open(fh, mode='r')
#roi_img = img.crop(box)

#img_byte_arr = io.BytesIO()
#roi_img.save(img_byte_arr, format='PNG')
#img_byte_arr = img_byte_arr.getvalue()


def convertToImages(input):
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'wb') as f:
        f.write(input)
    with open(tmp.name, "rb") as f:
        images = convert_from_bytes(input)
    
    i = 0
    result = []
    for image in images:
        print(f"Working on image [{i}]")
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        result.append(img_byte_arr.getvalue())
        i = i + 1
    return result
          

                      


# COMMAND ----------

# TEST CODE - commented out.

#taken = spark.table("nlp.documents.downloads").select("content").take(1)
#content = taken[0]["content"]
#result = convertToImages(content)

# COMMAND ----------

return_schema = ArrayType(BinaryType())

convertToImages_udf = udf(convertToImages, return_schema)

# COMMAND ----------

df = spark.table("nlp.documents.downloads").limit(1)

# COMMAND ----------

from pyspark.sql.functions import posexplode
df_exploded = df.withColumn("images", convertToImages_udf(col("content"))).select(col("path"), posexplode("images").alias("page", "image"))

# COMMAND ----------

df_exploded.write.format("delta").saveAsTable("nlp.documents.document_images")

# COMMAND ----------

images = convert_from_bytes(open("/tmp/2201.00013.pdf", "rb").read())

# COMMAND ----------

import numpy as np

model = lp.Detectron2LayoutModel(
            config_path ='lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config', # In model catalog
            label_map   = {0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"}, # In model`label_map`
            extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] # Optional
        )

# COMMAND ----------

# MAGIC %sh
# MAGIC add-apt-repository ppa:alex-p/tesseract-ocr-devel

# COMMAND ----------

# MAGIC %sh
# MAGIC apt install -y tesseract-ocr

# COMMAND ----------

ocr_agent = lp.TesseractAgent(languages='eng')

page = 1
#loop through each page
my_file = open("/tmp/text_block_2201.00013.txt","w")

for image in images:
    image = np.array(image)
    layout = model.detect(image)

    #titles = lp.Layout([b for b in layout if b.type == 'Title'])
    #for title in titles
    
    text_blocks = lp.Layout([b for b in layout if b.type == 'Title']) #loop through each text box on page.
    
    for block in text_blocks:
        segment_image = (block
                        .pad(left=5, right=5, top=5, bottom=5)
                        .crop_image(image))
        text = ocr_agent.detect(segment_image)
        block.set(text=text, inplace=True)
        
    for i, txt in enumerate(text_blocks.get_texts()):
        my_file.write(txt.strip())
        my_file.write(f".... Page {page}\n")
    page = page + 1

my_file.close()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp | grep text_block

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /tmp/text_block_2201.00013.txt
