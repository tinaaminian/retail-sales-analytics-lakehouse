import os 
from dotenv import load_dotenv

load_dotenv()

def get_db_config() -> dict:
    """
        Returns postgres connection settings
    """
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ["POSTGRES_PORT"]),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"]
    }