import os
from fastapi import UploadFile
import csv
import logging
from config.db_config import get_native_connection
from queries_sql.sql_queries import SQL_QUERIES

UPLOAD_DIR = "/opt/spark/app/data/uploads"

def process_csv_upload(file: UploadFile, table_name: str):
    """
    Saves uploaded CSV file to the local data/uploads directory.

    Args:
        file (UploadFile): File uploaded via API.
        table_name (str): Name of the table (used to name the file).
    """
    try:

        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        filename = f"{table_name}.csv"
        path = os.path.join(UPLOAD_DIR, filename)

        logging.info(f"Saving file to: {path}")

        with open(path, "wb") as f:
            content = file.file.read()
            f.write(content)

        file.file.close()

    except Exception as e:
        logging.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

def run_query(key: str):
    """
    Executes a predefined SQL query based on the given key.
    Args:
        key (str): The identifier for the SQL query defined in the SQL_QUERIES dictionary.
    Returns:
        list[dict]: List of records returned by the query as dictionaries.
    """
    query = SQL_QUERIES.get(key)
    if not query:
        raise ValueError(f"No query found for key: {key}")
    with get_native_connection() as conn:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            return cursor.fetchall()
