from src.ingestion.config import get_db_config
import  psycopg 
from pathlib import Path
from psycopg import sql



def extract_tables():
    tables= [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory"
    ]

    output_dir = Path("data/landing/postgres")

    with psycopg.connect(**get_db_config()) as conn:
        with conn.cursor() as cur:
            for table in tables:
                output_file = output_dir / f"{table}.csv"
                copy_query = sql.SQL(
                    "COPY {}.{} TO STDOUT "
                    "WITH (FORMAT CSV, HEADER TRUE)"
                ).format(
                    sql.Identifier("source"),
                    sql.Identifier(table)
                )
                with cur.copy(copy_query) as copy:
                    with output_file.open("wb") as file:
                        for chunk in copy:
                            file.write(chunk)

                   
    print(f"Extraction completed: {output_file}")

if __name__ == "__main__":
    extract_tables()