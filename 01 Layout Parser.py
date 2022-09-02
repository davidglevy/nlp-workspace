# Databricks notebook source
# MAGIC %sh
# MAGIC cd /tmp
# MAGIC wget https://poppler.freedesktop.org/poppler-22.09.0.tar.xz
# MAGIC 
# MAGIC tar xvf poppler-22.09.0.tar.xz

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install -y libnss3 libnss3-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install -y libcairo2-dev libjpeg-dev libgif-dev libopenjp2-7-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install -y cmake libblkid-dev e2fslibs-dev libboost-all-dev libaudit-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp
# MAGIC ls

# COMMAND ----------

# MAGIC %pip install layoutparser torchvision pdf2image

# COMMAND ----------

# MAGIC %pip install "git+https://github.com/facebookresearch/detectron2.git@v0.5#egg=detectron2"

# COMMAND ----------

# MAGIC %pip install "layoutparser[ocr]"

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/poppler-22.09.0
# MAGIC mkdir build
# MAGIC cd build/
# MAGIC cmake  -DCMAKE_BUILD_TYPE=Release   \
# MAGIC        -DCMAKE_INSTALL_PREFIX=/usr  \
# MAGIC        -DTESTDATADIR=$PWD/testfiles \
# MAGIC        -DENABLE_UNSTABLE_API_ABI_HEADERS=ON \
# MAGIC        ..
# MAGIC 
# MAGIC make
# MAGIC make install

# COMMAND ----------

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
