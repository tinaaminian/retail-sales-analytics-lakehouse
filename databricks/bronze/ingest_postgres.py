from pyspark.sql import SparkSession


#--------------------------------------
# Configuration
#--------------------------------------
CATALOG = "retail_sales_lakehouse"
VOLUME_PATH = f"/Volumes/{CATALOG}/landing/raw_files/postgres"

# Source tables names and their column definition
SOURCE_TABLES= {
    "customers":["customer_id","first_name","last_name","email","city","province","created_at","updated_at"],
    "products": ["product_id","sku","product_name","category","unit_price","is_active","created_at","updated_at"],
    "orders": ["order_id","customer_id","order_date","status","shipping_city","created_at","updated_at"],
    "order_items": ["order_item_id","order_id","product_id","quantity","unit_price","discount_amount","created_at","updated_at"],
    "payments": ["payment_id","order_id","payment_method","amount","payment_status","paid_at","created_at","updated_at"],
    "inventory": ["inventory_id","product_id","warehouse_code","stock_quantity","reorder_level","created_at","updated_at"],
}

# Ingest tables
def ingest_postgres_data(spark:SparkSession, table_name:str) -> None:

    # Construct source and destination
    columns = SOURCE_TABLES[table_name]
    source_path = f"{VOLUME_PATH}/{table_name}"
    target_table = f"{CATALOG}.bronze.{table_name}"

    # Build bronze schema
    column_definitions = ",\n".join(
        f"`{column}` STRING"
        for column in columns
    )
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table}(
            {column_definitions},
            ingested_at TIMESTAMP,
            source_system STRING,
            source_file STRING
        )
        USING DELTA
    """
    spark.sql(create_sql)

    # Construct COPY INTO statement
    copy_sql = f"""
        COPY INTO {target_table}
        FROM (
            SELECT *,
            current_timestamp() as ingested_at,
            'postgres' as source_system,
            _metadata.file_path as source_file
            FROM '{source_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS (
            'header' = 'true',
            'inferSchema' = 'false'
        )
    """
    spark.sql(copy_sql)

    


def main():
    spark = SparkSession.builder.getOrCreate()
    for table in SOURCE_TABLES:
        ingest_postgres_data(spark, table)

if __name__ == "__main__":
    main()
