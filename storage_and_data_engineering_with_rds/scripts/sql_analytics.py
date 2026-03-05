import logging
import pandas as pd
from config.db_connection import get_cursor
from config.s3_client_config import *

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Establish database connecttion
# -----------------------------------------------------------------------
cursor, conn = get_cursor()

# -----------------------------------------------------------------------
# Aggregations (SUM, AVG, COUNT) queries
# -----------------------------------------------------------------------
logging.info("Segment-wise aggregation query executing.")
agg_query = """
    SELECT COUNT(DISTINCT order_id) AS Total_orders, 
    SUM(sales) AS total_sales, 
    AVG(profit) AS avg_profit 
    FROM superstore_sales
"""
# cursor.execute(agg_query)
# result = cursor.fetchone()
agg_result = pd.read_sql(agg_query, conn)
agg_result.to_csv(f"s3://{BUCKET_NAME}/processed/aggregations_result.csv", index=False)
logging.info("Segment-wise aggregation query completed.")
logging.info(f"\n{agg_result.head()}")


# -----------------------------------------------------------------------
# GROUP BY and HAVING clauses
# -----------------------------------------------------------------------
logging.info("Segment-wise aggregation query executing.")
segment_agg_query = """
    SELECT segment, 
    COUNT(DISTINCT order_id) AS total_orders, 
    SUM(sales) AS total_sales, 
    AVG(profit) AS avg_profit 
    FROM superstore_sales
    GROUP BY segment
"""
# cursor.execute(segment_agg_query)
# segment_agg_result = cursor.fetchall()
segment_agg_df = pd.read_sql(segment_agg_query, conn)
segment_agg_df.to_csv(f"s3://{BUCKET_NAME}/processed/segment_aggregations_result.csv", index=False)
logging.info("Segment-wise aggregation query completed.")
logging.info(f"\n{segment_agg_df.head()}")

# -----------------------------------------------------------------------
# Date-based filtering
# -----------------------------------------------------------------------
logging.info("Date-based filtering query executing.")
date_filter_query = """
    SELECT order_id, order_date, sales 
    FROM superstore_sales
    WHERE YEAR(order_date) = 2023
"""
# cursor.execute(date_filter_query)
# date_filter_result = cursor.fetchall()
date_filter_df = pd.read_sql(date_filter_query, conn)
date_filter_df.to_csv(f"s3://{BUCKET_NAME}/processed/date_filtered_result.csv", index=False)
logging.info("Date-based filtering query completed.")
logging.info(f"\n{date_filter_df.head()}")

# -----------------------------------------------------------------------
# Sorting and ranking results
# -----------------------------------------------------------------------
logging.info("ranking and sorting query executing.")
ranking_query = """
    SELECT sub_category, 
    SUM(sales) AS total_sales,
    ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) AS sales_rank
    FROM superstore_sales
    GROUP BY sub_category
"""
ranking_query_df = pd.read_sql(ranking_query, conn)
ranking_query_df.to_csv(f"s3://{BUCKET_NAME}/processed/ranking_result.csv", index=False)
logging.info("ranking and sorting query completed.")
logging.info(f"\n{ranking_query_df.head(10)}")

# -----------------------------------------------------------------------
# Identifying top or bottom performers based on metrics
# -----------------------------------------------------------------------
logging.info("top performers query executing.")
top_performing_query = """
    SELECT
    region,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) AS sales_rank
    FROM superstore_sales
    GROUP BY region
"""
top_performing_query_df = pd.read_sql(top_performing_query, conn)
top_performing_query_df.to_csv(f"s3://{BUCKET_NAME}/processed/top_performing_result.csv", index=False)
logging.info("top performers query completed.")
logging.info(f"\n{top_performing_query_df.head(3)}")

# -----------------------------------------------------------------------
# Close the database connection
# -----------------------------------------------------------------------
logging.info("Script execution finished.")
cursor.close()
conn.close()
logging.info("Database connection closed.")