import dlt
import pyspark.sql.functions as F
from variables import *

SILVER_PRODUCTS = f"{SILVER_SCHEMA}.silver_products"
SILVER_PRODUCTS_CURRENT = f"{SILVER_SCHEMA}.silver_products_current"

@dlt.view(name="cleaned_products")
@dlt.expect("valid_price", "unit_price > 0")
def cleaned_products():
    return (
        dlt.read_stream(BRONZE_PRODUCTS)
        .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    )

dlt.create_auto_cdc_flow(
    target=SILVER_PRODUCTS,
    source="cleaned_products",
    keys=["product_id"],
    sequence_by=F.col("ingest_timestamp"),
    stored_as_scd_type=2,
)

@dlt.view(name=SILVER_PRODUCTS_CURRENT)
def silver_products_current():
    return dlt.read(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())
