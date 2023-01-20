-- Databricks notebook source
USE nlp.documents;

-- COMMAND ----------

SELECT path, count(*)
FROM document_images
GROUP BY path

-- COMMAND ----------

CREATE OR REPLACE TABLE expanded_content
AS
SELECT path, page, section_index, section.title, section.continuation, posexplode(section.texts) AS (text_index, text)
FROM
(SELECT path, page, posexplode(image_content) AS (section_index, section) from nlp.documents.pages)
ORDER BY path, page, section_index, text_index;

-- COMMAND ----------

SELECT path, page, section_index, title, count(text)
FROM expanded_content
WHERE title IS NOT NULL
GROUP BY path, page, section_index, title
ORDER BY path, page, section_index

-- COMMAND ----------

SELECT path, page, section_index, section.title, section.continuation, posexplode(section.texts) AS (text_index, text)
FROM
(SELECT path, page, posexplode(image_content) AS (section_index, section) from nlp.documents.pages)
ORDER BY path, page, section_index, text_index;
