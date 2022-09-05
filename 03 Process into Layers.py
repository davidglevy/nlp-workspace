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

df_exploded.write.format("delta").mode("overwrite").saveAsTable("nlp.documents.document_images")
