# Databricks notebook source
import numpy as np
import layoutparser as lp
import io
from PIL import Image



# COMMAND ----------

taken_df = spark.sql("SELECT image FROM nlp.documents.document_images WHERE path LIKE '%2208.14121'")
cached_taken_df = taken_df.cache()
taken = taken_df.collect()
page = taken[0]["image"]

# COMMAND ----------

from threading import Lock, Thread

class LockWrapper:

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

    model: lp.Detectron2LayoutModel

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

result = extractTitles(page)

# COMMAND ----------

from pyspark.sql.functions import udf, col, posexplode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, BooleanType

schema = ArrayType(
    StructType([
        StructField('title', BooleanType(), True),
        StructField('texts', ArrayType(StringType()), True),
        StructField('continuation', BooleanType(), True)
        ]
    )
)

extractTitles_udf = udf(extractTitles, schema)

# COMMAND ----------

display(cached_taken_df.repartition(16).withColumn("image_content", extractTitles_udf(col("image"))).select("image_content"))

# COMMAND ----------

df = spark.table("nlp.documents.document_images").limit(1)

# COMMAND ----------

from pyspark.sql.functions import length

display(df.withColumn("length_image", length("image")))

# COMMAND ----------

display(df)

# COMMAND ----------


df_titles = df.select(extractTitles_udf(col("image")))

# COMMAND ----------

display(df_titles)

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
