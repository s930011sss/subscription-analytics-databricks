# Databricks notebook source
# MAGIC %sql
# MAGIC WITH customer_metrics AS 
# MAGIC (
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   MIN(month) AS first_month,
# MAGIC   MAX(month) AS last_month,
# MAGIC   COUNT(DISTINCT month) AS active_months,
# MAGIC   SUM(mrr) AS total_mrr,
# MAGIC   AVG(mrr) AS avg_mrr,
# MAGIC   MAX(CASE WHEN actual_status = 'churned' THEN 1 ELSE 0 END) AS is_churned
# MAGIC FROM silver.fact_enriched
# MAGIC WHERE actual_status IN ('new','active','churned')
# MAGIC GROUP BY customer_id
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM customer_metrics
# MAGIC ORDER BY total_mrr DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;
# MAGIC USE gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.customer_segements AS
# MAGIC WITH customer_metrics AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(DISTINCT month) AS active_months,
# MAGIC     SUM(mrr) AS total_mrr,
# MAGIC     AVG(mrr) AS avg_mrr,
# MAGIC     MAX(CASE WHEN actual_status = 'churned' THEN 1 ELSE 0 END) AS is_churned
# MAGIC   FROM silver.fact_enriched
# MAGIC   WHERE actual_status IN ('new','active','churned')
# MAGIC   GROUP BY customer_id
# MAGIC ),
# MAGIC
# MAGIC quantile_based_segement AS 
# MAGIC (
# MAGIC   SELECT
# MAGIC     percentile_approx(avg_mrr, 0.8) AS p80,
# MAGIC     percentile_approx(avg_mrr, 0.5) AS p50
# MAGIC   FROM
# MAGIC     customer_metrics
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.active_months,
# MAGIC   c.total_mrr,
# MAGIC   c.avg_mrr,
# MAGIC   c.is_churned,
# MAGIC
# MAGIC   CASE 
# MAGIC       WHEN is_churned = 1 THEN 'Churned'
# MAGIC       WHEN c.avg_mrr >= q.p80 AND active_months >= 6 THEN 'High Value'
# MAGIC       WHEN c.avg_mrr >= q.p50 AND active_months >= 3 THEN 'Medium Value'
# MAGIC       ELSE 'Low Value'
# MAGIC   END AS value_segment
# MAGIC FROM customer_metrics c
# MAGIC CROSS JOIN quantile_based_segement q
# MAGIC ORDER BY total_mrr DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   value_segment,
# MAGIC   COUNT(*) AS customers,
# MAGIC   ROUND(AVG(avg_mrr),2) AS avg_mrr
# MAGIC FROM gold.customer_segements
# MAGIC GROUP BY value_segment
# MAGIC ORDER BY avg_mrr DESC;