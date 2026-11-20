""" Data quality rules
Every rule is written null-safe 
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ORDER_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_store_id": "store_id IS NOT NULL",
    "valid_order_ts": "order_ts IS NOT NULL",
    "has_items": "items IS NOT NULL AND size(items) > 0",
    "no_rescued_data": "_rescued_data IS NULL",
}

ORDER_ITEM_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "positive_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_unit_price": "unit_price IS NOT NULL AND unit_price >= 0",
}

PAYMENT_RULES = {
    "valid_payment_id": "payment_id IS NOT NULL",
    "valid_order_id": "order_id IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount >= 0",
    "valid_status": "status IN ('SUCCESS', 'FAILED')",
    "valid_payment_ts": "payment_ts IS NOT NULL"
}

SHIPMENT_RULES = {
    "valid_shipment_id": "shipment_id IS NOT NULL",
    "valid_order_id": "order_id IS NOT NULL",
    "valid_shipped_ts": "shipped_ts IS NOT NULL",
}

STORE_RULES = {
    "valid_store_id":"store_id IS NOT NULL",
}

def failed_rules(rules:dict) -> Column:
    checks = []

    for name, exp in rules.items():
        check = (
            F.when(
                F.coalesce(F.expr(exp), F.lit(False)),
                F.lit(None)
            ).otherwise(F.lit(name))
        )
        checks.append(check)
    
    result = F.array(*checks)
    result = F.filter(
        result,
        lambda x: x.isNotNull()
    )
    return result

def to_quarantine(df:DataFrame, entity:str, rules:dict) -> DataFrame:

    # Step1: Identify failed rules
    validated_df = df.withColumn(
        "failed_rules", failed_rules(rules)
    )

    # Step2: Keep only invalid rows
    invalid_df = validated_df.filter(
        F.size(F.col("failed_rules")) > 0
    )

    # Step3: Prepare the quarantine DataFrame
    quarantine_df = invalid_df.select(
        F.lit(entity).alias("entity"),
        F.col("failed_rules"),
        F.to_json(
            F.struct(*[F.col(c) for c in df.columns])
        ).alias("record"),
        F.col("_source_file"),
        F.current_timestamp().alias("_quarantined_ts")
    )
    return quarantine_df

    
