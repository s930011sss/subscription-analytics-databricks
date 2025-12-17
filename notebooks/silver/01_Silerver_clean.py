# Databricks notebook source
spark.table('default.subscription_customers')

# COMMAND ----------

spark.table('default.subscription_monthly_facts')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

#load tables
customers_bronze = spark.table('default.subscription_customers')
fact_bronze = spark.table('default.subscription_monthly_facts')

display(customers_bronze.limit(5))
display(fact_bronze.limit(5))

# COMMAND ----------

# ---- Silver: Clean customers table ----

customers_silver = (
    customers_bronze
    # trim string
    .withColumn('country', trim(col('country')))
    .withColumn('initial_plan', trim(col('initial_plan')))
    .withColumn('acquisition_channel', trim(col('acquisition_channel')))
    .withColumn('usage_segment', trim(col('usage_segment')))
    .withColumn('value_segment', trim(col('value_segment')))

    # standardize types
    .withColumn('singup_month', to_date(col('signup_month')))
    .withColumn('churn_month', to_date(col('churn_month')))
    .withColumn('signup_cac',col('signup_cac').cast('double'))
    .withColumn('is_annual',col('is_annual').cast('boolean'))

    # remove duplicate
    .dropDuplicates()
)

display(customers_silver.limit(5))
customers_silver.printSchema()

# COMMAND ----------

# ---- Silver: Clean Monthly table ----

fact_silver = (
    fact_bronze
    # trim string
    .withColumn('country', trim(col('country')))
    .withColumn('plan', trim(col('plan')))
    .withColumn('status', trim(col('status')))
    .withColumn('acquisition_channel', trim(col('acquisition_channel')))

    # standardize types
    .withColumn('month', to_date(col('month')))
    .withColumn('mrr', col('mrr').cast('double'))
    .withColumn('usage_hours', col('usage_hours').cast('double'))
    .withColumn('cost_to_serve', col('cost_to_serve').cast('double'))
    .withColumn('profit', col('profit').cast('double'))
    .withColumn('is_annual',col('is_annual').cast('boolean'))

    # remove duplicate
    .dropDuplicates()
)

display(fact_silver.limit(5))
fact_silver.printSchema()

# COMMAND ----------

facts_enriched = (
    fact_silver.alias('f')
    .join(
        customers_silver.alias('c'),
        on = 'customer_id',
        how = 'left'
    )
    .select(
        'f.*',
        'c.signup_month',
        'c.churn_month',
        'c.usage_segment',
        'c.value_segment'
    )
)
display(facts_enriched.limit(5))
facts_enriched.printSchema()

# COMMAND ----------

fact_silver_with_status = (
    facts_enriched
    .withColumn(
        'actual_status',
        when(col('churn_month') == col('month'), 'churned')
        .when(col('signup_month') == col('month'), 'new').otherwise('active')
    )
)
display(fact_silver_with_status.limit(20))

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Write Delta tables
(
    customers_silver
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('silver.customers')
)

(
    fact_silver
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('silver.fact')
)

(
    fact_silver_with_status
    .write
    .format('delta')
    .mode('overwrite')
    .option("overwriteSchema", "true")
    .saveAsTable('silver.fact_enriched')
)

print("Silver tables created")

# COMMAND ----------

display(spark.table('silver.customers').limit(5))
display(spark.table('silver.fact').limit(5))
display(spark.table('silver.fact_enriched').limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.fact limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customers limit 5;