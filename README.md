# Introduction

This repository contains various NLP related examples.

# Known Issues / Outstanding Tasks
There are a lot of todo's to make this better, here's a list of things to be aware of.

## Refactor Image Processing Pipeline
I need to do the following to make it easier to run the image processing pipeline

1. Ensure we have a cluster spun up by Terraform. This is currently blocked by a chicken/egg situation where we need to compile poppler as a DBFS dependency
2. Move into sub directory, this stuff should not be top level
3. Build cluster for Pytorch / Tesseract, refactor dependencies. Right now we're hit with the "non-deterministic dependency" bug in our Terraform provider.
4. Add README.md

## Improvements for Audio Pipeline
Audio pipeline is simpler than Image processing but still has a few improvements to do.

1. Add in some ML to the end of the pipeline (topic modelling)
2. Build some simple analytics.
3. Download mp3's once
4. Add README.md


