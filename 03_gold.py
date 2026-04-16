import dlt
import pyspark.sql.functions as F
from variables import *

SILVER_SALES = f"{SILVER_SCHEMA}.silver_sales_transactions"
SILVER_CUSTOMERS_CURRENT = f"{SILVER_SCHEMA}.silver_customers_current"
SILVER_PRODUCTS_CURRENT = f"{SILVER_SCHEMA}.silver_products_current"
SILVER_STORES_CURRENT = f"{SILVER_SCHEMA}.silver_stores_current"

@dlt.materialized_view(name=f"{GOLD_SCHEMA}.denormalized_sales_facts")
def denormalized_sales_facts():
    sales = dlt.read(SILVER_SALES)
    customers = dlt.read(SILVER_CUSTOMERS_CURRENT)
    products = dlt.read(SILVER_PRODUCTS_CURRENT)
    stores = dlt.read(SILVER_STORES_CURRENT)
    return (
        sales
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .join(stores, "store_id", "left")
    )

@dlt.materialized_view(name=f"{GOLD_SCHEMA}.gold_daily_sales_by_store")
def gold_daily_sales_by_store():
    return (
        dlt.read(f"{GOLD_SCHEMA}.denormalized_sales_facts")
        .groupBy(F.to_date("event_time").alias("sale_date"), "store_id", "store_name", "city")
        .agg(
            F.sum("total_amount").alias("daily_revenue"),
            F.count("transaction_id").alias("transaction_count"),
        )
    )

@dlt.materialized_view(name=f"{GOLD_SCHEMA}.gold_customer_lifetime_value")
def gold_customer_lifetime_value():
    return (
        dlt.read(f"{GOLD_SCHEMA}.denormalized_sales_facts")
        .groupBy("customer_id", "first_name", "last_name", "email")
        .agg(
            F.sum("total_amount").alias("lifetime_value"),
            F.count("transaction_id").alias("total_orders"),
            F.min("event_time").alias("first_purchase"),
            F.max("event_time").alias("last_purchase"),
        )
    )

@dlt.materialized_view(name=f"{GOLD_SCHEMA}.gold_product_performance")
def gold_product_performance():
    return (
        dlt.read(f"{GOLD_SCHEMA}.denormalized_sales_facts")
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("unit_price").alias("avg_selling_price"),
        )
    )
