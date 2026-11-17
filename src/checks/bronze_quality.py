
"""Quality checks for records in ShopStream Bronze tables."""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_SOURCES = (
    "customers_cdc",
    "products_cdc",
    "stores",
    "orders",
    "payments",
    "shipments",
)


# ===================================================
# 1. Rescued data validation
# ===================================================

def check_rescued_data(
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
    entity: str,
) -> DataFrame:
    """Count records containing rescued data in one Bronze table."""

    table_name = f"{catalog}.{bronze_schema}.{entity}"
    bronze = spark.table(table_name)

    # Ensure the rescued data column exists.
    if "_rescued_data" not in bronze.columns:
        raise ValueError(f"Missing _rescued_data column in {table_name}")

    # Count all records and records containing rescued data.
    return bronze.agg(
        F.lit(entity).alias("entity"),
        F.count("*").alias("total_rows"),
        F.count("_rescued_data").alias("rescued_rows"),
    ).withColumn(
        "status",
        F.when(
            F.col("rescued_rows") > 0, "FAIL"
        ).otherwise("PASS"),
    )


def check_all_rescued_data(
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
) -> DataFrame:
    """Run rescued-data checks across all Bronze entities."""

    results = []

    # Run the rescued-data check for each Bronze table.
    for entity in BRONZE_SOURCES:
        result = check_rescued_data(
            spark=spark,
            catalog=catalog,
            bronze_schema=bronze_schema,
            entity=entity,
        )
        results.append(result)

    if not results:
        raise ValueError("No Bronze entities configured.")

    # Combine all entity-level results into one DataFrame.
    combined = results[0]

    for result in results[1:]:
        combined = combined.unionByName(result)

    return combined


# ===================================================
# 2. Ingestion metadata validation
# ===================================================

def check_ingestion_metadata(
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
    entity: str,
) -> DataFrame:
    """Validate ingestion metadata fields for one Bronze table."""

    table_name = f"{catalog}.{bronze_schema}.{entity}"
    bronze = spark.table(table_name)

    # Ensure all expected ingestion metadata columns exist.
    required_cols = [
        "_source_file",
        "_file_modification_ts",
        "_batch_id",
        "_ingested_at",
    ]

    missing_cols = []

    for col in required_cols:
        if col not in bronze.columns:
            missing_cols.append(col)

    if missing_cols:
        raise ValueError(
            f"Missing metadata columns in {table_name}: {missing_cols}"
        )

    # Check for missing or empty source file paths.
    invalid_source_file = (
        F.col("_source_file").isNull()
        | (F.trim(F.col("_source_file")) == "")
    )

    # Extract the expected batch ID from the source filename.
    expected_batch_id = F.regexp_extract(
        F.col("_source_file"),
        r"batch_(\d+)\.json$",
        1,
    ).cast("int")

    # Check for missing, negative, or mismatched batch IDs.
    invalid_batch_id = (
        F.col("_batch_id").isNull()
        | (F.col("_batch_id") < 0)
        | expected_batch_id.isNull()
        | (F.col("_batch_id") != expected_batch_id)
    )

    # Check for missing file modification timestamps.
    invalid_file_ts = F.col("_file_modification_ts").isNull()

    # Check for missing or inconsistent ingestion timestamps.
    invalid_ingested_at = (
        F.col("_ingested_at").isNull()
        | (
            F.col("_ingested_at")
            < F.col("_file_modification_ts")
        )
    )

    # Count failures for each metadata validation rule independently.
    result = bronze.agg(
        F.lit(entity).alias("entity"),
        F.count("*").alias("total_rows"),
        F.count(
            F.when(invalid_source_file, 1)
        ).alias("invalid_source_file"),
        F.count(
            F.when(invalid_batch_id, 1)
        ).alias("invalid_batch_id"),
        F.count(
            F.when(invalid_file_ts, 1)
        ).alias("invalid_file_ts"),
        F.count(
            F.when(invalid_ingested_at, 1)
        ).alias("invalid_ingested_at"),
    )

    # Fail the entity if at least one metadata validation rule fails.
    return result.withColumn(
        "status",
        F.when(
            (F.col("invalid_source_file") > 0)
            | (F.col("invalid_batch_id") > 0)
            | (F.col("invalid_file_ts") > 0)
            | (F.col("invalid_ingested_at") > 0),
            "FAIL",
        ).otherwise("PASS"),
    )


def check_all_ingestion_metadata(
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
) -> DataFrame:
    """Run ingestion metadata checks across all Bronze entities."""

    results = []

    # Validate ingestion metadata for each Bronze table.
    for entity in BRONZE_SOURCES:
        result = check_ingestion_metadata(
            spark=spark,
            catalog=catalog,
            bronze_schema=bronze_schema,
            entity=entity,
        )
        results.append(result)

    if not results:
        raise ValueError("No Bronze entities configured.")

    # Combine all entity-level results into one DataFrame.
    combined = results[0]

    for result in results[1:]:
        combined = combined.unionByName(result)

    return combined
