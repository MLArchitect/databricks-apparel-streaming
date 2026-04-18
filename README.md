# Databricks Apparel Streaming Pipeline

End-to-end streaming data pipeline built on Databricks Delta Live Tables, PySpark, and Unity Catalog — implementing a medallion architecture (Bronze → Silver → Gold) for apparel retail analytics.

## Architecture


## DLT Pipeline Files

| Layer | Purpose |
|---|---|
| `01_bronze.py` | Bronze layer: Raw data ingestion from source files |
| `02A_silver.py` | Silver layer: Sales data cleaning and transformation |
| `02B_silver.py` | Silver layer: Customer dimension with SCD Type 2 |
| `02C_silver.py` | Silver layer: Product dimension with SCD Type 2 |
| `02D_silver.py` | Silver layer: Store dimension with SCD Type 2 |
| `03_gold.py` | Gold layer: Business-ready aggregations and analytics |
| `variables.py` | Shared catalog, schema, and path variables |

## Key Features

- Streaming ingestion using Databricks Auto Loader with exactly-once semantics
- DLT expect_or_fail to enforce primary key integrity at Bronze
- SCD Type 2 for customer and product dimensions — full historical change tracking
- Materialized Gold views automatically refreshed when upstream Silver data changes
- Unity Catalog governance: lineage, access control, and audit logs

## Tech Stack

Azure Databricks · Delta Live Tables · PySpark · Auto Loader · Delta Lake · Unity Catalog · ADLS Gen2

## Portfolio

[View full project writeup](https://mlarchitect.github.io/portfolio/databricks_apparel_streaming/)
