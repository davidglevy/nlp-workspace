# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nlp.audio.audio_raw;
# MAGIC DROP TABLE IF EXISTS nlp.audio.audio_converted_parts;
# MAGIC DROP TABLE IF EXISTS nlp.audio.audio_converted;
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS nlp.default;
# MAGIC DROP DATABASE IF EXISTS nlp.audio;
# MAGIC 
# MAGIC DROP CATALOG IF EXISTS nlp;
