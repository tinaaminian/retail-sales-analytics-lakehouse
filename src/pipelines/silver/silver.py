""" Silver layer part1: clean, deduplicate and quality checks based on business events.
Bad rows are dropped from silver by expectaitons and copied to ops.quarantine_records rather than losing data silently.
"""
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("src_path"))

from transformations.silver_quality import (
    ORDER_ITEM_RULES,
    ORDER_RULES,
    PAYMENT_RULES,
    SHIPMENT_RULES,
    STORE_RULES,
    to_quarantine,
)
from transformations.silver_transformations import (
    clean_orders,
    clean_payments,
    clean_shipments,
    clean_stores,
    dedupe,
    explode_order_items,
)

CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema")
SILVER = spark.conf.get("silver_schema")
OPS = spark.conf.get("ops_schema")
DEDUP_WINDOW = "6 hours"  # how long we remember keys to catch duplicates

def fq(schema: str, table: str) -> str:
    """Fully qualified table name: catalog.schema.table"""
    return f"{CATALOG}.{schema}.{table}"

QUARANTINE = fq(OPS, "quarantine_records")

# --- orders ------------------------------------------------------------------

def silver_orders_source():
    orders = clean_orders(spark.readStream.table(fq(BRONZE, "orders")))
    return dedupe(orders, ["order_id"], "order_ts", DEDUP_WINDOW )

@dp.table(name= fq(SILVER,"orders"), comment= "Clean, deduplicated orders", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(ORDER_RULES)
def silver_orders():
    return silver_orders_source()

@dp.table(name= fq(SILVER, "order_items"), comment="One row per order line", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(ORDER_ITEM_RULES)
def silver_order_items():
    return explode_order_items(spark.readStream.table(fq(SILVER, "orders")))


# --- payments & shipment ------------------------------------------------------

def silver_payments_source():
    payments = clean_payments(spark.readStream.table(fq(BRONZE, "payments")))
    return dedupe(payments, ["payment_id"], "payment_ts", DEDUP_WINDOW)

@dp.table(name= fq(SILVER, "payments"), comment="Clean, deduplicated payments", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(PAYMENT_RULES)
def silver_payments():
    return silver_payments_source()

def silver_shipments_source():
    shipments = clean_shipments(spark.readStream.table(fq(BRONZE, "shipments")))
    return dedupe(shipments, ["shipment_id"], "shipped_ts", DEDUP_WINDOW)


@dp.table(name=fq(SILVER, "shipments"), comment="Clean, deduplicated shipments", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(SHIPMENT_RULES)
def silver_shipments():
    return silver_shipments_source()

# --- store ------------------------------------------------------

def silver_store_source():
    stores = clean_stores(spark.readStream.table(fq(BRONZE, "stores")))
    return stores.dropDuplicates(["store_id"])

@dp.table(name= fq(SILVER, "stores"), comment="Clean, deduplicated stores", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(STORE_RULES)
def silver_stores():
    return silver_store_source()

# --- quarantine: one table, one append flow per entity ----------------------------------
dp.create_streaming_table(name= QUARANTINE, comment="rows rejected by silver data quality rules")

@dp.append_flow(target=QUARANTINE, name="quarantine_orders")
def quarantine_orders():
    return to_quarantine(clean_orders(spark.readStream.table(fq(BRONZE, "orders"))), "orders", ORDER_RULES)

@dp.append_flow(target=QUARANTINE, name= "quarantine_order_items")
def quarantine_order_items():
    items = explode_order_items(spark.readStream.table(fq(SILVER, "orders")))
    return to_quarantine(items, "order_items", ORDER_ITEM_RULES)

@dp.append_flow(target=QUARANTINE, name="quarantine_payments")
def quarantine_payments():
    return to_quarantine(clean_payments(spark.readStream.table(fq(BRONZE, "payments"))), "payments", PAYMENT_RULES)


@dp.append_flow(target=QUARANTINE, name="quarantine_shipments")
def quarantine_shipments():
    shipments = clean_shipments(spark.readStream.table(fq(BRONZE, "shipments")))
    return to_quarantine(shipments, "shipments", SHIPMENT_RULES)

@dp.append_flow(
    target=QUARANTINE,
    name="quarantine_stores",
)
def quarantine_stores():
    stores = clean_stores(
        spark.readStream.table(fq(BRONZE, "stores"))
    )

    return to_quarantine(
        stores,
        "stores",
        STORE_RULES,
    )