from pyspark.sql import  functions as F
from pyspark.sql import DataFrame, SparkSession


BRONZE_SOURCES = (
    "customers_cdc",
    "products_cdc",
    "stores",
    "orders",
    "payments",
    "shipments",
)

def source_file_counts(spark:SparkSession, landing_path:str, entity:str) -> DataFrame:
    """ Count records per source json file. """

    path = f"{landing_path}/{entity}"

    # Read source files as text so each JSON line is counted as one record.
    # Capture the full file path to identify the origin of each record.
    source = (
        spark.read.format("text").load(path)
        .select(
            F.col("value"),
            F.col("_metadata.file_path").alias("source_file")
        )
    )

    # Aggregate record counts separately for each source file.
    return(
        source.groupBy("source_file")
        .agg(
            F.count("*").alias("source_rows")
        )
    )

def bronze_file_count(spark:SparkSession, catalog:str, bronze_schema:str, entity:str) -> DataFrame:
    """ Count Bronze rows grouped by original source file. """

    # Read the Bronze Delta table populated by the ingestion pipeline.
    table_name = f"{catalog}.{bronze_schema}.{entity}"
    bronze = spark.table(table_name)

    return(
        bronze.select(
            F.col("_source_file").alias("source_file")
        ).groupBy("source_file").agg(
            F.count("*").alias("bronze_rows")
        )
    )

def reconcile_entity(
    spark:SparkSession,
    landing_path:str,
    entity:str,
    catalog:str,
    bronze_schema:str
    ) -> DataFrame:
    """ Compare source and bronze record counts for one entity. """

    source = source_file_counts(spark,landing_path,entity)
    bronze = bronze_file_count(spark, catalog, bronze_schema, entity)

    # Preserve files from both sides to detect missing or unexpected files.
    comparison = source.join(
        bronze,
        on = "source_file",
        how = "full_outer"
        )

    # Classify each file based on its presence and record-count consistency.
    return comparison.select(
        F.lit(entity).alias("entity"),
        F.col("source_file"),
        F.col("source_rows"),
        F.col("bronze_rows"),
        (
            F.when(F.col("source_rows").isNull(), "NOT_IN_SOURCE")
            .when(F.col("bronze_rows").isNull(), "MISSING_IN_BRONZE")
            .when(F.col("source_rows") != F.col("bronze_rows"), "COUNT_MISMATCH")
            .otherwise("PASS")
        ).alias("status")
    )



def display_all(spark:SparkSession, landing_path:str, catalog:str, bronze_schema:str,) -> DataFrame:
    """ Run reconciliation for all bronze entities. """

    results = []
    # Reconcile each entity independently and collect its result DataFrame.
    for entity in BRONZE_SOURCES:
        result = reconcile_entity(
            spark = spark,
            landing_path=landing_path,
            entity=entity,
            catalog=catalog,
            bronze_schema=bronze_schema
        )
        results.append(result)
    
     # Stack all entity results into one consolidated reconciliation DataFrame.
    combined = results[0]
    for result in results[1:]:
       combined =  combined.unionByName(result)
    
    return combined