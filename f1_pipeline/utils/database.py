# f1_pipeline/database.py
"""
PostgreSQL database utilities with SQLAlchemy engine management.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def get_engine(db_config: dict):
    """Create SQLAlchemy engine from database configuration."""
    return create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
    )


# def load_to_postgres(df: pd.DataFrame, table_name: str, db_config: dict):
#     """Load DataFrame into PostgreSQL table with replace strategy."""
#     logger.info(f"Loading data into PostgreSQL table: {table_name}")
#     engine = get_engine(db_config)
#     df.to_sql(table_name, con=engine, index=False, if_exists="replace")
#     logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
#

# def load_to_postgres(df: pd.DataFrame, table_name: str, db_config: dict):
#     """Load DataFrame into PostgreSQL table with truncate/insert strategy."""
#     logger.info(f"Loading data into PostgreSQL table: {table_name}")
#     engine = get_engine(db_config)
#
#     with engine.begin() as conn:
#         # Check if table exists
#         if conn.dialect.has_table(conn, table_name):
#             # Truncate existing data, keep table structure
#             conn.execute(f"TRUNCATE TABLE {table_name}")
#             df.to_sql(table_name, con=conn, index=False, if_exists="append")
#         else:
#             # Create new table
#             df.to_sql(table_name, con=conn, index=False, if_exists="fail")
#
#     logger.info(f"Successfully loaded {len(df)} rows into {table_name}")



def load_to_postgres(df: pd.DataFrame, table_name: str, db_config: dict):
    """Load DataFrame into PostgreSQL table with truncate/insert strategy."""
    logger.info(f"Loading data into PostgreSQL table: {table_name}")
    engine = get_engine(db_config)

    with engine.begin() as conn:
        # Check if table exists
        if conn.dialect.has_table(conn, table_name):
            # Truncate existing data, keep table structure
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            df.to_sql(table_name, con=conn, index=False, if_exists="append")
        else:
            # Create new table
            df.to_sql(table_name, con=conn, index=False, if_exists="fail")

    logger.info(f"Successfully loaded {len(df)} rows into {table_name}")