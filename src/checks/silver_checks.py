from pyspark.sql import functions as F

# Check1: Schema validation
def check_schema(df,expected_schema):
    actual_schema = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    errors = {}

    for column, expected_type in expected_schema.items():
        actual_type = actual_schema.get(column)

        if actual_type != expected_type:
            errors[column] = {
                "expected": expected_type,
                "actual": actual_type,
            }
    return errors


# Check2: Null detection
def check_not_null(df, columns):
    """Count rows containing NULL in any required column."""

    condition = None

    for column in columns:
        null_check = F.col(column).isNull()

        condition = (
            null_check if condition is None
            else condition | null_check
        )

    return df.filter(condition).count() if condition is not None else 0

# Check3: business key uniqueness

def check_duplicates(df, keys):
    """Count duplicate records beyond one per business key."""

    duplicates = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
    )

    result = duplicates.agg(
        F.sum(F.col("count") - 1).alias("duplicate_count")
    ).first()

    return result["duplicate_count"] or 0

# Check 4: reconciliation

def check_key_coverage(bronze, silver, quarantine):
    """Count Bronze keys absent from both Silver and quarantine."""

    bronze_keys = bronze.select("key").distinct()

    covered_keys = (
        silver.select("key")
        .unionByName(quarantine.select("key"))
        .distinct()
    )

    missing = bronze_keys.join(
        covered_keys,
        on="key",
        how="left_anti",
    )

    return missing.count()

