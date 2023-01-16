# Databricks notebook source
# MAGIC %md
# MAGIC # Build Poppler for PDF to Image Conversion
# MAGIC 
# MAGIC If we're building pipelines to take raw PDF images, we need to use pdf2image library and require
# MAGIC the supporting poppler operating system dependency. This will build it and install it.

# COMMAND ----------

dbutils.fs.mkdirs("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/test")

# COMMAND ----------

dbutils.fs.ls("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/test")

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
# MAGIC wget https://poppler.freedesktop.org/poppler-22.09.0.tar.xz
# MAGIC 
# MAGIC tar xvf poppler-22.09.0.tar.xz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/poppler-22.09.0
# MAGIC mkdir build
# MAGIC cd build/
# MAGIC cmake  -DCMAKE_BUILD_TYPE=Release ENABLE_BOOST=OFF   \
# MAGIC        -DCMAKE_INSTALL_PREFIX=/tmp/usr  \
# MAGIC        -DTESTDATADIR=$PWD/testfiles \
# MAGIC        -DENABLE_UNSTABLE_API_ABI_HEADERS=ON \
# MAGIC        ..
# MAGIC 
# MAGIC make clean
# MAGIC make
# MAGIC make install

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp
# MAGIC tar czf poppler_usr.tar.gz ./usr

# COMMAND ----------

dbutils.fs.mkdirs("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/poppler")
dbutils.fs.cp("file:///tmp/poppler_usr.tar.gz", "abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/poppler")
dbutils.fs.ls("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/poppler")

# COMMAND ----------

# MAGIC %sh
# MAGIC dbfs ls "abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/poppler"

# COMMAND ----------


dbutils.fs.cp("abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/poppler/poppler_usr.tar.gz", "file:///tmp/build/poppler_usr.tar.gz")

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/build
# MAGIC tar xvzf poppler_usr.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/build
# MAGIC cp -pr ./usr /

# COMMAND ----------

dbutils.fs.cp("file:///tmp/poppler_usr.tar.gz", "/poppler/poppler_usr.tar.gz")

# COMMAND ----------

dbutils.fs.ls("/poppler/")
