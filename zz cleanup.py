# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nlp.documents.downloads;
# MAGIC DROP TABLE IF EXISTS nlp.documents.document_images;
# MAGIC DROP TABLE IF EXISTS nlp.documents.document_titles;
# MAGIC DROP TABLE IF EXISTS nlp.documents.audio_raw;
# MAGIC DROP TABLE IF EXISTS nlp.documents.audio_converted_parts;
# MAGIC 
# MAGIC 
# MAGIC DROP DATABASE nlp.default;
# MAGIC DROP DATABASE nlp.documents;
# MAGIC 
# MAGIC DROP CATALOG nlp;
