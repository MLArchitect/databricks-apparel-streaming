import dlt
import pyspark.sql.functions as F
from variables import *

SILVER_STORES = f"{SILVER_SCHEMA}.silver_stores"
SILVER_STORES_CURRENT = f"{SILVER_SCHEMA}.silver_stores_current"

@dlt.view(name="cleaned_stores")
@dlt.expect("valid_store_name", "store_name is not null")
def cleaned_stores():
    return (
        dlt.read_stream(BRONZE_STORES)
        .withColumn("store_name", F.initcap(F.trim(F.col("store_name"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
    )

dlt.create_auto_cdc_flow(
    target=SILVER_STORES,
    source="cleaned_stores",
    keys=["store_id"],
    sequence_by=F.col("ingest_timestamp"),
    stored_as_scd_type=2,
)

@dlt.view(name=SILVER_STORES_CURRENT)
def silver_stores_current():
    return dlt.read(SILVER_STORES).filter(F.col("__END_AT").isNull())
