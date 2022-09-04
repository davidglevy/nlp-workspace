# Databricks notebook source
import layoutparser as lp
import cv2

# COMMAND ----------

from urllib import request

request.urlretrieve("https://arxiv.org/ftp/arxiv/papers/2201/2201.00013.pdf", "/tmp/2201.00013.pdf")

# COMMAND ----------

from pdf2image import convert_from_bytes

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
