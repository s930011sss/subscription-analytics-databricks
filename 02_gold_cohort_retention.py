# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   signup_month,
# MAGIC   month,
# MAGIC   actual_status
# MAGIC FROM
# MAGIC   SILVER.fact_enriched
# MAGIC ORDER BY customer_id, month
# MAGIC LIMIT 50;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     signup_month AS cohort_month,
# MAGIC     month,
# MAGIC     -- cal gap of month
# MAGIC     (YEAR(month) - YEAR(signup_month)) *12
# MAGIC     + (MONTH(month) - MONTH(signup_month)) AS cohort_index
# MAGIC   FROM silver.fact_enriched
# MAGIC )
# MAGIC
# MAGIC select *
# MAGIC from base
# MAGIC where cohort_index >= 0 
# MAGIC ORDER BY customer_id, cohort_index
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cohort_base AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     signup_month AS cohort_month
# MAGIC   FROM silver.fact_enriched
# MAGIC   WHERE actual_status = 'new'
# MAGIC )
# MAGIC SELECT
# MAGIC   cohort_month,
# MAGIC   COUNT(DISTINCT customer_id) AS cohort_size
# MAGIC FROM cohort_base
# MAGIC GROUP BY cohort_month
# MAGIC ORDER BY cohort_month ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gols;
# MAGIC USE gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.cohort_retention AS
# MAGIC WITH cohort_base AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     signup_month AS cohort_month,
# MAGIC     actual_status,
# MAGIC     (YEAR(month) - YEAR(signup_month)) *12
# MAGIC     + (MONTH(month) - MONTH(signup_month)) AS cohort_index
# MAGIC   FROM silver.fact_enriched
# MAGIC ),
# MAGIC
# MAGIC active_users AS (
# MAGIC   SELECT
# MAGIC       cohort_month,
# MAGIC       cohort_index,
# MAGIC       COUNT(DISTINCT customer_id) as active_users
# MAGIC   FROM cohort_base
# MAGIC   WHERE cohort_index >= 0
# MAGIC   AND actual_status IN ('new','active')
# MAGIC   GROUP BY cohort_month,cohort_index
# MAGIC ),
# MAGIC
# MAGIC cohort_size AS (
# MAGIC   SELECT 
# MAGIC     signup_month as cohort_month,
# MAGIC     COUNT(DISTINCT customer_id) as cohort_size
# MAGIC   FROM silver.fact_enriched
# MAGIC   WHERE actual_status = 'new'
# MAGIC   GROUP BY signup_month
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.cohort_month,
# MAGIC   a.cohort_index,
# MAGIC   c.cohort_size,
# MAGIC   a.active_users,
# MAGIC   a.active_users / c.cohort_size as retention_rate
# MAGIC FROM 
# MAGIC   active_users a
# MAGIC JOIN cohort_size c
# MAGIC ON a.cohort_month = c.cohort_month
# MAGIC ORDER BY a.cohort_month, a.cohort_index;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from gold.cohort_retention
# MAGIC order by cohort_month, cohort_index