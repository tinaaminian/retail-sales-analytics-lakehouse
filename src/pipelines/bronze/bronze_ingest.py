""" Bronze layer: raw, append-only ingestion of every landing entity with auto loader"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")

BRONZE_SOURCES = {
    "customers_cdc": "Customer CDC events",
    "products_cdc": "Product CDC events",
    "stores": "Store reference data",
    "orders": "Order events with nested line items",
    "payments": "Payment events; may arrive hours after the order",
    "shipments": "Shipment events",
}

def create_bronze_table(entity: str, comment: str) -> None:

    @dp.table(name = entity, comment = comment, table_properties = {"quality": "bronze"})
    @dp.expect("no_rescued_data", "_rescued_data is NULL")

    def bronze():
        return(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{LANDING_PATH}/{entity}")
        ).withColumns({
            "_source_file": F.col("_metadata.file_path"),
            "_file_modification_ts": F.col("_metadata.file_modification_time"),
            "_batch_id": F.regexp_extract(
                F.col("_metadata.file_path"),
                r"batch_(\d+)",
                1
                ).cast("int"),
            "_ingested_at": F.current_timestamp()
        })

for entity, comment in BRONZE_SOURCES.items():
    create_bronze_table(entity, comment)


