import argparse
import sys 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F



def main():
    """ Run bronze checks and fail the Job if any validation fails. """

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required = True)
    parser.add_argument("--bronze-schema", required = True)
    parser.add_argument("--landing-path", required = True)
    parser.add_argument("--project-root", required = True)
    args = parser.parse_args()

    sys.path.insert(0,args.project_root)
    from src.checks.bronze_checks import display_all
    from src.checks.bronze_quality import (
    check_all_ingestion_metadata,
    check_all_rescued_data
)

    # Creating spark session
    spark = SparkSession.builder.getOrCreate()

    # source_to_bronze file reconciliation
    reconciliation = display_all(
        spark = spark,
        landing_path = args.landing_path,
        catalog = args.catalog,
        bronze_schema = args.bronze_schema
    )

    # rescued-data validation
    rescued = check_all_rescued_data(
        spark= spark,
        catalog= args.catalog,
        bronze_schema= args.bronze_schema
    )

    # ingestion metadata validation
    metadata = check_all_ingestion_metadata(
        spark = spark,
        catalog = args.catalog,
        bronze_schema = args.bronze_schema
    )

    # Report results in the Job output.
    print("=== Source-to-Bronze Reconciliation ===")
    reconciliation.show(truncate=False)

    print("=== Rescued Data Validation ===")
    rescued.show(truncate=False)

    print("=== Ingestion Metadata Validation ===")
    metadata.show(truncate=False)

    # count failed validations.
    failures = (
        reconciliation.filter(F.col("status") != "PASS").count()
        + rescued.filter(F.col("status") != "PASS").count()
        + metadata.filter(F.col("status") != "PASS").count()
    )
    if failures > 0:
        raise ValueError(f"Bronze validation failed: {failures} failed checks.")
    
    print("All bronze quality checks passed")



if __name__ == "__main__":
    main()