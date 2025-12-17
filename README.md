# Subscription Analytics on Databricks

This project demonstrates an end-to-end subscription analytics workflow built on Databricks, focusing on transforming raw subscription data into business-ready insights such as MRR, retention, customer segmentation, and LTV.

The goal of this project is not only to compute metrics, but to practice analytics engineering principles: clear data modeling, reproducible transformations, and interpretable business logic.

---

## Project Overview

As part of my self-study, I designed and implemented a complete analytics pipeline to answer realistic subscription business questions:

- How is subscription revenue evolving over time?
- What drives growth or decline (new, expansion, contraction, churn)?
- How does retention differ across customer cohorts?
- Which customers generate the most long-term value?

The project is built entirely on Databricks using SQL and Python notebooks, following a layered Lakehouse-style design.

---

## Architecture

This project follows a simplified Lakehouse architecture:

- **Direct Ingestion**  
  Raw CSV data is ingested directly into Databricks tables (no separate Bronze layer).

- **Silver Layer**  
  Cleaned and standardized tables with consistent schemas and enriched attributes.

- **Gold Layer**  
  Analytics-ready fact and dimension tables designed for reporting and decision-making.

All business logic is implemented at the data model level, ensuring dashboards remain lightweight and consistent.

---

## Data Layers

### Silver Layer
The Silver layer focuses on data cleaning, normalization, and enrichment.

Typical transformations include:
- Schema standardization
- Date normalization
- Joining customer attributes into monthly subscription facts
- Preparing data for analytical use

Tables in this layer serve as stable inputs for downstream models.

---

### Gold Layer
The Gold layer contains business-facing models and metrics, including:

- **MRR Metrics**
  - Active MRR
  - New / Expansion / Contraction / Churn MRR
  - Net New MRR

- **Cohort Retention**
  - Monthly retention rates by signup cohort
  - Cohort index tracking (Month 0–12)

- **Customer Segmentation**
  - Quantile-based segmentation (High / Medium / Low Value)
  - Churned customer identification

- **Customer LTV**
  - Estimated lifetime value per customer
  - Aggregated LTV by segment

These tables are designed to be directly consumable by dashboards or BI tools.

---

## Dashboard

A Databricks SQL dashboard was built on top of the Gold tables to provide an executive view of subscription performance.

The dashboard includes:
- Executive KPIs (Active MRR, Net New MRR, New MRR, Churn MRR)
- MRR trend and revenue decomposition
- Cohort retention analysis
- LTV comparison across customer segments

Dashboard screenshots are available in:

