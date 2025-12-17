# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.customer_segements
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;
# MAGIC USE gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.customer_ltv AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   value_segment,
# MAGIC   active_months,
# MAGIC   avg_mrr,
# MAGIC
# MAGIC   -- LTV calcualte
# MAGIC   avg_mrr * active_months as ltv
# MAGIC FROM
# MAGIC   gold.customer_segements;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   value_segment,
# MAGIC   COUNT(*) AS customers,
# MAGIC   ROUND(AVG(ltv),2) as avg_ltv,
# MAGIC   ROUND(MAX(ltv),2) as max_ltv
# MAGIC FROM
# MAGIC   gold.customer_ltv
# MAGIC GROUP BY
# MAGIC   value_segment
# MAGIC ORDER BY
# MAGIC   avg_ltv DESC;