# Databricks notebook source
# MAGIC %sql
# MAGIC select
# MAGIC   customer_id,
# MAGIC   month,
# MAGIC   actual_status,
# MAGIC   mrr 
# MAGIC from silver.fact_enriched
# MAGIC order by customer_id, month
# MAGIC limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC with base as (
# MAGIC   select
# MAGIC     customer_id,
# MAGIC     month,
# MAGIC     actual_status,
# MAGIC     mrr,
# MAGIC     lag(mrr) over (
# MAGIC       partition by customer_id
# MAGIC       order by month
# MAGIC     ) as prev_mrr
# MAGIC   from silver.fact_enriched
# MAGIC )
# MAGIC select *
# MAGIC from base
# MAGIC order by customer_id,month
# MAGIC limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;
# MAGIC USE gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.mrr_metrics AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     month,
# MAGIC     actual_status,
# MAGIC     mrr,
# MAGIC     LAG(mrr) OVER (
# MAGIC       PARTITION BY customer_id
# MAGIC       ORDER BY month
# MAGIC     ) AS prev_mrr
# MAGIC   FROM silver.fact_enriched
# MAGIC ),
# MAGIC
# MAGIC classified AS (
# MAGIC   SELECT
# MAGIC     month,
# MAGIC
# MAGIC     -- Active MRR
# MAGIC     SUM(CASE
# MAGIC     WHEN actual_status IN ('active','new') THEN mrr 
# MAGIC     ELSE 0 END) AS active_mrr,
# MAGIC
# MAGIC     --NEW MRR
# MAGIC     SUM(CASE
# MAGIC     WHEN actual_status = 'new' THEN mrr
# MAGIC     ELSE 0 END) AS new_mrr,
# MAGIC     
# MAGIC     --Expansion MRR
# MAGIC     SUM(CASE
# MAGIC         WHEN prev_mrr IS NOT NULL
# MAGIC         AND mrr > prev_mrr
# MAGIC         AND actual_status = 'active'
# MAGIC         THEN mrr - prev_mrr
# MAGIC         ELSE 0 END) AS expension_mrr,
# MAGIC
# MAGIC     -- Contraction MRR
# MAGIC     SUM(CASE 
# MAGIC         WHEN prev_mrr IS NOT NULL
# MAGIC         AND mrr < prev_mrr 
# MAGIC         AND actual_status = 'active'
# MAGIC         THEN prev_mrr - mrr
# MAGIC         ELSE 0 END) AS contraction_mrr,
# MAGIC
# MAGIC     -- Churn MRR
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN actual_status = 'churned'
# MAGIC         THEN mrr
# MAGIC         ELSE 0 END) AS churn_mrr
# MAGIC
# MAGIC    FROM base
# MAGIC    group by month     
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     month,
# MAGIC     active_mrr,
# MAGIC     new_mrr,
# MAGIC     expension_mrr,
# MAGIC     contraction_mrr,
# MAGIC     churn_mrr,
# MAGIC     (new_mrr + expension_mrr - contraction_mrr - churn_mrr) AS net_new_mrr
# MAGIC FROM classified
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.mrr_metrics
# MAGIC ORDER BY month;