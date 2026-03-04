import pyodbc
import os
from dotenv import load_dotenv

# -----------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------
load_dotenv()

def get_connection():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )
    return conn


def get_cursor():
    conn = get_connection()
    return conn.cursor(), conn