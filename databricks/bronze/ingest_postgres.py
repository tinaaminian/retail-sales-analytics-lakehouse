from pyspark.sql import functions as F
from pyspark.sql import SparkSession
#-----------------------------------
# configuration
#-----------------------------------

catalog = "retail_sales_lakehouse"
volume_path = f"/Volumes/{catalog}/landing/raw_files"

source_tables = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory"
)

#-----------------------------------
# Ingest one postgres table
#-----------------------------------
def ingest_postgres_table(spark:SparkSession, table_name:str) -> None:
    #Validate the table name
    if table_name not in source_tables:
        raise ValueError(f"unsupported table:{table_name}")
    

    #Define source and destination
    source_path = f"{volume_path}/{table_name}.csv"
    target_table= f"{catalog}.bronze.{table_name}"

    #Read csv data 
    df = (
        spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","false")
        .load(source_path)
    )

    #Add ingestion metadata
    bronze_df = (
        df.select(
            "*"
        ).withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_system", F.lit("postgres"))
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

    #Write the initail full load
    bronze_df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    print(f"success:{target_table}")

def main():
    spark = SparkSession.builder.getOrCreate()
    for table_name in source_tables:
        ingest_postgres_table(spark,table_name)

if __name__ == "__main__":
    main()