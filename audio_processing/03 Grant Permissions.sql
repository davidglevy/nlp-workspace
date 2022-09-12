-- Databricks notebook source
-- MAGIC %sql
-- MAGIC REVOKE SELECT ON nlp_demo.audio.audio_converted FROM `david.g.levy@gmail.com`;
-- MAGIC REVOKE USAGE ON SCHEMA nlp_demo.audio FROM `david.g.levy@gmail.com`;
-- MAGIC REVOKE USAGE ON CATALOG nlp_demo FROM `david.g.levy@gmail.com`;
-- MAGIC 
-- MAGIC DROP TABLE IF EXISTS nlp_demo.audio.audio_converted;
-- MAGIC 
-- MAGIC DROP SCHEMA IF EXISTS nlp_demo.default;
-- MAGIC DROP SCHEMA IF EXISTS nlp_demo.audio;
-- MAGIC 
-- MAGIC DROP CATALOG IF EXISTS nlp_demo;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS nlp_demo;
CREATE SCHEMA IF NOT EXISTS nlp_demo.audio;
CREATE TABLE nlp_demo.audio.audio_converted
AS SELECT * FROM nlp.audio.audio_converted;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC GRANT USAGE ON CATALOG nlp_demo TO `david.g.levy@gmail.com`;
-- MAGIC GRANT USAGE ON SCHEMA nlp_demo.audio TO `david.g.levy@gmail.com`;
-- MAGIC GRANT SELECT ON nlp_demo.audio.audio_converted TO `david.g.levy@gmail.com`;
