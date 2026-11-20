""" Clean CDC events for customers and products"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformations.silver_transformations import to_ts


def _clean_envelope(df: DataFrame) -> DataFrame:
    """ Columns every CDC feed has: opetation, sequence number and event time"""

    return (
        df.withColumn("operation", F.upper(F.trim(F.col("operation"))))
        .withColumn("sequence_num", F.expr("try_cast(sequence_num as BIGINT)"))
        .withColumn("event_ts", to_ts("event_ts"))
    )


def clean_customer_cdc(df:DataFrame) -> DataFrame:
   
    df = _clean_envelope(df)
    df = (
        df.withColumn("customer_id", F.expr("try_cast(customer_id as BIGINT)"))
        .withColumn("email" , F.lower(F.trim("email")))
        .withColumn("tier", F.upper(F.trim("tier")))
        .withColumn("province", F.upper(F.trim("province")))
        .withColumn("city", F.trim("city"))
    )
    if "loyalty_points" in df.columns:
        df = df.withColumn("loyalty_points",F.expr("try_cast(loyalty_points as INT)"))
            
    return df


def clean_products_cdc(df:DataFrame) -> DataFrame:
    
    return (
        _clean_envelope(df)
        .withColumn("product_id", F.expr("try_cast(product_id as BIGINT)"))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("price", F.expr("try_cast(price as DECIMAL(10,2))"))
    )