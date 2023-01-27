#!/bin/bash


apt-get install -y libnss3 libnss3-dev
apt-get install -y libcairo2-dev libjpeg-dev libgif-dev libopenjp2-7-dev
apt-get install -y cmake libblkid-dev e2fslibs-dev libboost-all-dev libaudit-dev
#add-apt-repository ppa:alex-p/tesseract-ocr-devel
apt-get install -y tesseract-ocr

mkdir /tmp/build
cd /tmp/build
cp /dbfs/poppler/poppler_usr.tar.gz /tmp/build/poppler_usr.tar.gz
tar xvzf poppler_usr.tar.gz
cp -pr ./usr /


