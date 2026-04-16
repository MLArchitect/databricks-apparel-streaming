import dlt
import pyspark.sql.functions as F
from variables import *

SILVER_SALES = f"{SILVER_SCHEMA}.silver_sales_transactions"
SILVER_RETURNS = f"{SILVER_SCHEMA}.silver_returns_transactions"

@dlt.view(name="cleaned_sales")
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_total_amount", "total_amount > 0")
def cleaned_sales():
    return (
        dlt.read_stream(BRONZE_SALES)
        .filter(F.col("transaction_type").isin("sale", "return"))
    )

@dlt.table(name=SILVER_SALES, comment="Cleaned sales transactions (sales only)")
def silver_sales_transactions():
    return dlt.read_stream("cleaned_sales").filter(F.col("transaction_type") == "sale")

@dlt.table(name=SILVER_RETURNS, comment="Cleaned return transactions")
def silver_returns_transactions():
    return dlt.read_stream("cleaned_sales").filter(F.col("transaction_type") == "return")
