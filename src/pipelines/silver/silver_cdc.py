""" SDC type1 and type2 for customers and products, using AUTO CDC.
From one change feed we build two targets per entity:
<base> _current -> SCD type1: latest per entity
<base> _history -> SCD type2: evety version with _start_at / _end_at
"""

from os import eventfd
import sys

from pyspark.errors import QueryContext
from pyspark.sql import catalog, functions as F
from pyspark import pipelines as dp
from pyspark.sql.utils import try_partitioning_remote_functions

sys.path.append(spark.conf.get("src_path"))  # same header as silver_events.py

from pipelines.silver.silver import CATALOG, QUARANTINE
from transformations.customers_products_cdc import clean_customer_cdc, clean_products_cdc
from transformations.silver_quality import CUSTOMER_CDC_RULES, PRODUCT_CDC_RULES, to_quarantine, valid_condition
from transformations.silver_transformations import dedupe

CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema")
SILVER = spark.conf.get("silver_schema")
OPS = spark.conf.get("ops_schema")

QUARANTINE = f"{CATALOG}.{OPS}.quarantine_records"

CDC_CONTROL_COLS = ["operation", "sequence_num", "_rescued_data", "_source_file", "_ingested_at"]
DEDUP_DELAY = "6 hours"

def fq(schema: str, table:str) -> str:
    return f"{CATALOG}.{schema}.{table}"


def register_cdc_entity(base:str, key:str, clean_fn, rules:dict, track_cols: list[str]) -> None:
    """ Declare the view, quarantine flows and AUTO CDC flows for one entity"""

    bronze_table = f"{base}_cdc"
    clean_view = f"{bronze_table}_clean",
    valid_view = f"{bronze_table}_valid"

    @dp.view(name= clean_view)
    def _clean():
        return clean_fn(spark.readStream.table(fq(BRONZE, bronze_table)))

    @dp.view(name = valid_view)
    def _valid():
        deduped = dedupe(
            spark.readStream.table(clean_view),
            keys = [key, "sequence_num"],
            event_time_col = "event_ts",
            delay = DEDUP_DELAY
            )
        return deduped.where(valid_condition(rules))
    
    @dp.append_flow(target = QUARANTINE, name = f"quarantine_{bronze_table}")
    def _quarantine():
        return to_quarantine(spark.readStream.table(clean_view), bronze_table, rules)
    

    current = fq(SILVER, F"{base}_current")
    history = fq(SILVER, f"{base}_history")
    dp.create_streaming_table(name = current)
    dp.create_streaming_table(name = history)

    # SCD Type1: one row per key, overwitten by the latest event
    dp.create_auto_cdc_flow(
        target = current,
        source = valid_view,
        keys = [key],
        sequence_by = F.col("event_ts"),
        apply_as_deletes = F.expr("opetation = 'DELETE'"),
        except_column_list= CDC_CONTROL_COLS,
        stored_as_scd_type=1
    )

    # SCD Type2: a new version whenever a tracked column changes
    dp.create_auto_cdc_flow(
        target = history,
        aource = valid_view,
        keys = [key],
        sequence_by= F.col("event_ts"),
        apply_as_deletes= F.expr("opetation = 'DELETE'"),
        except_column_list=CDC_CONTROL_COLS,
        stored_as_scd_type=2,
        track_history_column_list=track_cols
    )

register_cdc_entity(
    base="customers",
    key="customer_id",
    clean_fn=clean_customer_cdc,
    rules=CUSTOMER_CDC_RULES,
    # A change to loyalty_points updates the current version in place, not a new version.
    # Tracking it would create a new history row on every points change.
    track_cols=["tier", "city", "province", "email"],
)

register_cdc_entity(
    base="products",
    key="product_id",
    clean_fn=clean_products_cdc,
    rules=PRODUCT_CDC_RULES,
    track_cols=["price", "category"],
)