# Databricks notebook source
# MAGIC %md
# MAGIC # Summary before Weekend
# MAGIC I managed to get this working, the most efficient way to run this is:
# MAGIC 
# MAGIC 1. Ensure we use a singleton, this ensures we don't create a layoutparser or ocr processor for each loop
# MAGIC 2. Repartition to the number of executors. This ensures we don't have concurrent requests on same GPU. If running single node, this is 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## For GPU: Get the CUDA, layoutparser and torch versions.
# MAGIC For GPU, CUDA is 11.3, so we need torch 1.10 and 
# MAGIC 
# MAGIC See https://github.com/facebookresearch/detectron2/blob/main/INSTALL.md
# MAGIC 
# MAGIC 
# MAGIC     python -m pip install detectron2 -f \
# MAGIC     https://dl.fbaipublicfiles.com/detectron2/wheels/cu113/torch1.10/index.html
# MAGIC 
# MAGIC ## For non-GPU clusters, use this:
# MAGIC 
# MAGIC pip install git+https://github.com/facebookresearch/detectron2.git@v0.6#egg=detectron2

# COMMAND ----------

# MAGIC %pip install detectron2 -f https://dl.fbaipublicfiles.com/detectron2/wheels/cu113/torch1.10/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## For this version of detectron, we need torch 1.10 and torchvision 0.11.3
# MAGIC 
# MAGIC This comes with DBR 10.4 ML runtime

# COMMAND ----------

#%sh
#ls -l /usr/local | grep cuda

# COMMAND ----------

import numpy as np
import layoutparser as lp
import io
from PIL import Image
import torch


# COMMAND ----------

taken_df = spark.sql("SELECT * FROM nlp.documents.document_images WHERE path LIKE '%2208.14121'")
taken = taken_df.collect()
page = taken[0]["image"]

# COMMAND ----------

import torch
print(torch.__version__)

# COMMAND ----------

from threading import Lock, Thread

class LockWrapper:
    """
    This class enables us to wrap Lock objects used to create Singletons before
    they are serialized with cloudpickle.
    """

    def __init__(self):
        self.lock = Lock()

    def __getstate__(self):
        return {}
    
    def __setstate__(self, oldstate):
        self.lock = Lock()


class SingletonMeta(type):
    """
    A singleton PdfContentExtractor.

    https://refactoring.guru/design-patterns/singleton/python/example#example-1
    """
    _instances = {}
    _lockWrapper: LockWrapper = LockWrapper()

    def __call__(cls, *args, **kwargs):
        with cls._lockWrapper.lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]

class PdfExtractor(metaclass=SingletonMeta):

    def __init__(self):
        # Initialize the model.
        # config_path ='lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config', # In model catalog
        self.model = lp.Detectron2LayoutModel(
            config_path ='/dbfs/fwc/layoutparser/config.yaml', # In DBFS
            label_map   = {0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"}, # In model`label_map`
            extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] # Optional
        )
        # Load the OCR Agent
        self.ocr_agent = lp.TesseractAgent(languages='eng')

    def extractTitles(self, input):
        # Convert back into image
        stream = io.BytesIO(input) 
        image = Image.open(stream)

        # Process
        image_np = np.array(image)
        layout = self.model.detect(image_np)


        text_blocks = lp.Layout([b for b in layout if (b.type == 'Title' or b.type == 'Text')])
        
        print(f"We found [{len(text_blocks)}] text blocks")



        for block in text_blocks:
            segment_image = (block
                            .pad(left=5, right=5, top=0, bottom=0)
                            .crop_image(image_np))
            text = self.ocr_agent.detect(segment_image)
            block.set(text=text, inplace=True)

        text_blocks = sorted(text_blocks, key=lambda x: x.block.y_1)

        
        sections = []

        section = None


        for i, text_block in enumerate(text_blocks):
            print(text_block)
            txt = text_block.text
            block_type = text_block.type
            stripped = txt.strip()
            if not stripped:
                continue
            
            if block_type == 'Title':
                # We need a new section
                section = {'title': stripped, 'texts': []}
                sections.append(section)
            elif block_type == 'Text' and section:
                section['texts'].append(stripped)
            elif block_type == 'Text' and not section:
                section = {'continuation': True, 'texts': []}
                sections.append(section)
                section['texts'].append(stripped)
        return sections



# COMMAND ----------

class PdfExtractor():

    def __init__(self):
        # Initialize the model.
        # config_path ='lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config', # In model catalog
        self.model = lp.Detectron2LayoutModel(
            config_path ='/dbfs/fwc/layoutparser/config.yaml', # In DBFS
            label_map   = {0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"}, # In model`label_map`
            extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] # Optional
        )
        # Load the OCR Agent
        self.ocr_agent = lp.TesseractAgent(languages='eng')

    def extractTitles(self, input):
        # Convert back into image
        stream = io.BytesIO(input) 
        image = Image.open(stream)

        # Process
        image_np = np.array(image)
        layout = self.model.detect(image_np)


        text_blocks = lp.Layout([b for b in layout if (b.type == 'Title' or b.type == 'Text')])
        
        print(f"We found [{len(text_blocks)}] text blocks")



        for block in text_blocks:
            segment_image = (block
                            .pad(left=5, right=5, top=0, bottom=0)
                            .crop_image(image_np))
            text = self.ocr_agent.detect(segment_image)
            block.set(text=text, inplace=True)

        text_blocks = sorted(text_blocks, key=lambda x: x.block.y_1)

        
        sections = []

        section = None


        for i, text_block in enumerate(text_blocks):
            print(text_block)
            txt = text_block.text
            block_type = text_block.type
            stripped = txt.strip()
            if not stripped:
                continue
            
            if block_type == 'Title':
                # We need a new section
                section = {'title': stripped, 'texts': []}
                sections.append(section)
            elif block_type == 'Text' and section:
                section['texts'].append(stripped)
            elif block_type == 'Text' and not section:
                section = {'continuation': True, 'texts': []}
                sections.append(section)
                section['texts'].append(stripped)
        return sections



# COMMAND ----------

# TODO Shift this into a Pandas UDF function.

def extractTitles(input):
    # Create the extractor
    extractor = PdfExtractor()
    print(f"Extractor id: {id(extractor)}")
    print(f"Input length: {len(input)}")

    result = extractor.extractTitles(input)

    for section in result:
        if 'title' in section:
            print(f"Title: {section['title']}\n")
        for i, text in enumerate(section['texts']):
            print(f"Text {i}: {text}\n")

    return result



# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

def extractTitlesPd(inputs: pd.Series) -> pd.Series:

    extractor = PdfExtractor()

    inputs_list = inputs.tolist()

    results = []

    for input in inputs_list:
        result = extractor.extractTitles(input)
        results.append(result)
    
    return pd.Series(results)



# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install -y tesseract-ocr
# MAGIC #apt-get install -y libtesseract-dev

# COMMAND ----------

result = extractTitles(page)

# COMMAND ----------

from pyspark.sql.functions import udf, col, posexplode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, BooleanType

schema = ArrayType(
    StructType([
        StructField('title', StringType(), True),
        StructField('texts', ArrayType(StringType()), True),
        StructField('continuation', BooleanType(), True)
        ]
    )
)

extractTitles_udf = udf(extractTitles, schema)

#extractTitlesPd_udf = pandas_udf(extractTitlesPd, returnType=schema)

# COMMAND ----------

page_content_df = taken_df.repartition(1).withColumn("image_content", extractTitles_udf(col("image")))

# COMMAND ----------

page_content_df.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable('nlp.documents.pages')
