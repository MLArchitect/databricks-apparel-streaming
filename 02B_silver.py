import dlt
import pyspark.sql.functions as F
from variables import *

SILVER_CUSTOMERS = f"{SILVER_SCHEMA}.silver_customers"
SILVER_CUSTOMERS_CURRENT = f"{SILVER_SCHEMA}.silver_customers_current"

@dlt.view(name="cleaned_customers")
@dlt.expect("valid_email", "email is not null")
def cleaned_customers():
    return (
        dlt.read_stream(BRONZE_CUSTOMERS)
        .filter(~F.col("email").contains("test"))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
    )

dlt.create_auto_cdc_flow(
    target=SILVER_CUSTOMERS,
    source="cleaned_customers",
    keys=["customer_id"],
    sequence_by=F.col("ingest_timestamp"),
    stored_as_scd_type=2,
)

@dlt.view(name=SILVER_CUSTOMERS_CURRENT)
def silver_customers_current():
    return dlt.read(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())
