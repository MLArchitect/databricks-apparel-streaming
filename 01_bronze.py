import dlt
import pyspark.sql.functions as F
from datetime import datetime, timezone
from variables import *

@dlt.table(name=BRONZE_SALES, comment="Raw sales data ingested from landing zone")
@dlt.expect_or_fail("valid_transaction_id", "transaction_id is not null")
def bronze_sales():
    return (
        spark.readStream.format("delta").load(RAW_SALES_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("transaction_id", F.col("transaction_id").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("store_id", F.col("store_id").cast("int"))
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )

@dlt.table(name=BRONZE_CUSTOMERS, comment="Raw customer data ingested from landing zone")
@dlt.expect_or_fail("valid_customer_id", "customer_id is not null")
def bronze_customers():
    return (
        spark.readStream.format("delta").load(RAW_CUSTOMERS_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
    )

@dlt.table(name=BRONZE_PRODUCTS, comment="Raw product data ingested from landing zone")
@dlt.expect_or_fail("valid_product_id", "product_id is not null")
def bronze_products():
    return (
        spark.readStream.format("delta").load(RAW_PRODUCTS_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("unit_cost", F.col("unit_cost").cast("double"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
    )

@dlt.table(name=BRONZE_STORES, comment="Raw store data ingested from landing zone")
@dlt.expect_or_fail("valid_store_id", "store_id is not null")
def bronze_stores():
    return (
        spark.readStream.format("delta").load(RAW_STORES_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("store_id", F.col("store_id").cast("int"))
    )
