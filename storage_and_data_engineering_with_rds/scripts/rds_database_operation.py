import logging
import pandas as pd
from config.db_connection import get_cursor
from config.s3_client_config import BUCKET_NAME
from scripts.download_data import download_file


logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Establish a connection to the RDS SQL Server database using pyodbc
# -----------------------------------------------------------------------
cursor, conn = get_cursor()

# -----------------------------------------------------------------------
# Create the 'superstore_sales' table if it doesn't exist
# -----------------------------------------------------------------------
create_table_query = """
IF NOT EXISTS (
    SELECT * FROM sys.tables 
    WHERE name = 'superstore_sales'
)
BEGIN
    CREATE TABLE superstore_sales (
        order_id VARCHAR(255) PRIMARY KEY,
        order_date DATE,
        ship_date DATE,
        customer_id VARCHAR(255),
        segment VARCHAR(255),
        region VARCHAR(255),
        country VARCHAR(255),
        category VARCHAR(255),
        sub_category VARCHAR(255),
        sales DECIMAL(10, 2),
        quantity INT,
        discount DECIMAL(10, 2),
        profit DECIMAL(10, 2),
        processing_days INT
    )
END
"""
cursor.execute(create_table_query) 
conn.commit()
logging.info("Table 'superstore_sales' created successfully.")

# -----------------------------------------------------------------------
# Download cleaned data from s3
# -----------------------------------------------------------------------
logging.info("Downloading cleaned data from S3...")
download_file("download/cleaned_superstore_sales.csv", "processed/cleaned_superstore_sales.csv", BUCKET_NAME)
logging.info("Data downloaded successfully from S3.")

# -----------------------------------------------------------------------
# Bulk insert data into the 'superstore_sales' table
# -----------------------------------------------------------------------
logging.info("Bulk inserting data into the table...")
df = pd.read_csv("download/cleaned_superstore_sales.csv")

cursor.fast_executemany = True
try:
    insert_query = """
    INSERT INTO superstore_sales (
        order_id, order_date, ship_date,
        customer_id, segment, region,
        country, category, sub_category,
        sales, quantity, discount, profit, processing_days
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_query, df.values.tolist())
    conn.commit()
    logging.info("Data inserted successfully into 'superstore_sales' table.")
except Exception as e:
    logging.error(f"Error inserting data: {e}")
    conn.rollback()

# -----------------------------------------------------------------------
# Read and filter records from the 'superstore_sales' table
# -----------------------------------------------------------------------
logging.info("Reading table data...")
select_query = "SELECT TOP 5 * FROM superstore_sales"
cursor.execute(select_query)
rows = cursor.fetchall()
for row in rows:
    logging.info(row)

logging.info("Filtering records with sales > 1000...")
filter_query = "SELECT * FROM superstore_sales WHERE sales > 1000"
cursor.execute(filter_query)
filtered_rows = cursor.fetchall()
for row in filtered_rows:
    logging.info(row)

# -----------------------------------------------------------------------
# Update records in the 'superstore_sales' table
# -----------------------------------------------------------------------
logging.info("Updating records with discount > 0.5...")
update_query = "UPDATE superstore_sales SET discount = 0.5 WHERE discount > 0.5"
cursor.execute(update_query)
logging.info("Records updated successfully.")

# -----------------------------------------------------------------------
# Delete records based on a condition
# -----------------------------------------------------------------------
logging.info("Deleting records with profit < 0...")
delete_query = "DELETE FROM superstore_sales WHERE profit < 0"
cursor.execute(delete_query)
logging.info("Records deleted successfully.")

# -----------------------------------------------------------------------
# Close the database connection
# -----------------------------------------------------------------------
cursor.close()
conn.close()
logging.info("Database connection closed.")