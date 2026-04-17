# Databricks Apparel Streaming Pipeline

End-to-end streaming data pipeline built on Databricks Delta Live Tables, PySpark, and Unity Catalog — implementing a medallion architecture (Bronze → Silver → Gold) for apparel retail analytics.

## Architecture

| Layer | Purpose |
|---|---|
| 00_landing | Raw streaming files in Unity Catalog Volume |
| 01_bronze | Auto Loader ingestion, schema enforcement, primary key validation |
| 02_silver | Cleaning, SCD Type 2, data quality expectations |
| 03_gold | Aggregations and materialized views |

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
