import logging
import traceback
import pandas as pd
from sqlalchemy import create_engine
from typing import Dict

def build_connection_string(conn_id: str, creds: Dict[str, str]) -> str:
    """Build SQLAlchemy connection string based on connection ID suffix."""
    servername = creds.get("servername")
    database = creds.get("database")
    port = creds.get("port")
    username = creds.get("username")
    password = creds.get("password")

    if conn_id.endswith("_mysql"):
        return f"mysql+pymysql://{username}:{password}@{servername}:{port}/{database}"
    elif conn_id.endswith("_gpsql"):
        return f"postgresql+psycopg2://{username}:{password}@{servername}:{port}/{database}"
    else:
        raise ValueError(f"Unsupported connection type: {conn_id}")

def get_db_engine(conn_str: str):
    """Create and return SQLAlchemy engine."""
    return create_engine(conn_str)

def get_table_count(engine, table_name: str) -> int:
    """Return count of rows in the table."""
    query = f"SELECT COUNT(1) AS count FROM {table_name}"
    try:
        df = pd.read_sql(query, engine)
        return df["count"].iloc[0] if not df.empty else 0
    except Exception:
        logging.error(f"Failed to get count for {table_name}:\n{traceback.format_exc()}")
        return 0

def get_sample_records(engine, table_name: str, limit: int = 10) -> pd.DataFrame:
    """Fetch sample records from a table."""
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    try:
        return pd.read_sql(query, engine)
    except Exception:
        logging.error(f"Failed to fetch records from {table_name}:\n{traceback.format_exc()}")
        return pd.DataFrame()
