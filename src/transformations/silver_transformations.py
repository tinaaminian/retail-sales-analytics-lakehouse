""" Silver transformations: pure DataFrame -> DataFrame 
every function can be unit tested.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

ITEM_SCHEMA = ArrayType(
    StructType([
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True)

    ])
)

META_COLS = ["_source_file", "_ingested_at"]

def to_ts(col_name: str) -> Column:
    """ Safely parse timestamp strings, returning null for invalid values"""
    return F.try_to_timestamp(F.col(col_name))

def dedupe(df: DataFrame, keys:list, event_time_col:str, delay:str) -> DataFrame:
    if df.isStreaming:
        return df.withWatermark(event_time_col,delay).dropDuplicatesWithinWatermark(keys)
    return df.dropDuplicates(keys)


def clean_orders(df:DataFrame) -> DataFrame:
    return df.select(
        F.expr("try_cast(order_id as BIGINT)").alias("order_id"),
        F.expr("try_cast(customer_id as BIGINT)").alias("customer_id"),
        F.expr("try_cast(store_id as INT)").alias("store_id"),
        to_ts("order_ts").alias("order_ts"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),

        #convert json string into ARRAY<STRUCT>
        F.from_json(F.col("items"), ITEM_SCHEMA).alias("items"),
        F.col("_rescued_data"),
        *META_COLS
    )

def explode_order_items(orders:DataFrame) -> DataFrame:
    return(
        orders.select(
            "order_id",
            "order_ts",
            F.posexplode("items").alias("pos","item"),
            *META_COLS
        )
        .select(
            "order_id",
            (F.col("pos")+1).alias("line_number"),
            F.expr("try_cast(item.product_id as BIGINT)").alias("product_id"),
            F.expr("try_cast(item.quantity as INT)").alias("quantity"),
            F.expr("try_cast(item.unit_price as DECIMAL(10,2))").alias("unit_price"),
            "order_ts",
            *META_COLS
        ).withColumn(
            "line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)")
        )
    )

def clean_payments(df:DataFrame) -> DataFrame:
    return df.select(
        F.expr("try_cast(payment_id as BIGINT)").alias("payment_id"),
        F.expr("try_cast(order_id as BIGINT)").alias("order_id"),
        F.expr("try_cast(amount as decimal(10,2))").alias("amount"),
        F.upper(F.trim(F.col("method"))).alias("method"),
        F.upper(F.trim(F.col("status"))).alias("status"),
        to_ts("payment_ts").alias("payment_ts"),
        *META_COLS
    )

def clean_shipments(df:DataFrame) -> DataFrame:
    return df.select(
        F.expr("try_cast(shipment_id as BIGINT)").alias("shipment_id"),
        F.expr("try_cast(order_id as BIGINT)").alias("order_id"),
        F.trim(F.col("carrier")).alias("carrier"),
        to_ts("shipped_ts").alias("shipped_ts"),
        *META_COLS
    )

def clean_stores(df:DataFrame) -> DataFrame:
    return df.select(
        F.expr("try_cast(store_id as INT)").alias("store_id"),
        F.trim(F.col("store_name")).alias("store_name"),
        F.trim(F.col("city")).alias("city"),
        F.upper(F.trim(F.col("region"))).alias("region"),
        F.col("_rescued_data"),
        *META_COLS
    )