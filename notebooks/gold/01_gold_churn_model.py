# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists gold;
# MAGIC use gold;
# MAGIC
# MAGIC -- step 1: cal monthly active / churn users
# MAGIC
# MAGIC create or replace table gold.churn_metrics as 
# MAGIC with monthly_stats as (
# MAGIC   select
# MAGIC     month,
# MAGIC     COUNT(CASE WHEN actual_status = 'active' THEN 1 END) As active_customers,
# MAGIC     COUNT(CASE WHEN actual_status = 'churned' THEN 1 END) AS churned_customers,
# MAGIC     COUNT(CASE WHEN actual_status = 'new' THEN 1 END) AS new_customers
# MAGIC     from silver.fact_enriched
# MAGIC     GROUP BY month
# MAGIC ),
# MAGIC
# MAGIC -- step 2: cal previous month active (log)
# MAGIC monthly_with_prev AS (
# MAGIC   SELECT 
# MAGIC     month,
# MAGIC     active_customers,
# MAGIC     churned_customers,
# MAGIC     new_customers,
# MAGIC     LAG(active_customers, 1) OVER (ORDER BY month) AS prev_month_active
# MAGIC   FROM monthly_stats
# MAGIC )
# MAGIC
# MAGIC -- step 3: churn rate = churned / prev_month_active
# MAGIC select
# MAGIC   month,
# MAGIC   new_customers,
# MAGIC   active_customers,
# MAGIC   churned_customers,
# MAGIC   prev_month_active,
# MAGIC   CASE 
# MAGIC     WHEN prev_month_active = 0 THEN 0
# MAGIC     ELSE churned_customers / prev_month_active
# MAGIC   END AS churn_rate
# MAGIC FROM monthly_with_prev

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.churn_metrics ORDER BY month;