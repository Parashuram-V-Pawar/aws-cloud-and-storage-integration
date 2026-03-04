import pandas as pd
import logging
from config.s3_client_config import *

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Functin to Read CSV from S3
# -----------------------------------------------------------------------
def read_csv_s3(bucket = BUCKET_NAME, s3_key = S3_RAW_UPLOAD_KEY):
    logging.info("Reading data from s3....")
    s3_path = f"s3://{bucket}/{s3_key}"
    data = pd.read_csv(s3_path)
    logging.info("Read successful....")
    return data

# -----------------------------------------------------------------------
# Function to clean data
# -----------------------------------------------------------------------
def data_cleaning(data):
    logging.info("Data cleaning started...")
    data.columns = data.columns.str.strip().str.lower().str.replace(" ", "_")
    data = data.drop(columns=["row_id"], errors="ignore")
    data = data.drop_duplicates()
    data["order_date"] = pd.to_datetime(data["order_date"], errors="coerce")
    data["ship_date"] = pd.to_datetime(data["ship_date"], errors="coerce")
    data.dropna(subset=["order_id", "order_date", "ship_date"], inplace=True)
    numeric_cols = ["sales", "quantity", "discount", "profit"]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")
    fill_values = {
        "customer_name": "Undefined",
        "segment": "Undefined",
        "region": "Undefined",
        "country": "Undefined",
        "category": "Undefined",
        "sub_category": "Undefined",
        "sales": data["sales"].mean(),
        "quantity": 0,
        "discount": 0,
        "profit": 0
    }
    data.fillna(value=fill_values, inplace=True)
    data["region"] = data["region"].str.title()
    data = data[data["sales"] >= 0]
    data = data[data["quantity"] > 0]
    data = data[data["discount"] >= 0]
    data["processing_days"] = (
        data["ship_date"] - data["order_date"]
    ).dt.days
    logging.info("Data cleaning finished.")
    return data

# -----------------------------------------------------------------------
# Function to upload cleaned data back to S3
# -----------------------------------------------------------------------
def upload_cleaned_data(data, bucket_name = BUCKET_NAME, s3_key = S3_PROCESSED_UPLOAD_KEY):
    logging.info("Uploading cleaned data to S3...")
    data.to_csv(f"s3://{bucket_name}/{s3_key}")
    logging.info(f"Upload successful: s3://{bucket_name}/{s3_key}")

# -----------------------------------------------------------------------
# Summary metrics generation function
# -----------------------------------------------------------------------
def generate_summary_metrics(data, bucket_name = BUCKET_NAME):
    logging.info("Generating summary metrics...")
    total_sales = data["sales"].sum()
    total_profit = data["profit"].sum()
    summary_metrics = pd.DataFrame({
        "total_revenue": [total_sales],
        "total_profit": [total_profit],
        "total_orders": [data["order_id"].nunique()],
        "total_customers": [data["customer_name"].nunique()],
        "total_quantity_sold": [data["quantity"].sum()],
        "average_order_value": [
            data.groupby("order_id")["sales"].sum().mean()
        ],
        "profit_margin_percent": [
            (total_profit / total_sales) * 100 if total_sales != 0 else 0
        ]
    })
    # Save to S3
    summary_metrics.to_parquet(f"s3://{bucket_name}/processed/summary_metrics.parquet", index=False)
    logging.info(f"Summary metrics generated and uploaded to S3: s3://{bucket_name}/processed/summary_metrics.parquet")

# -----------------------------------------------------------------------
# Group-wise Aggregations functions
# -----------------------------------------------------------------------
def sales_by_region(data, bucket_name = BUCKET_NAME):
    logging.info("Generating sales by region...")
    sales_by_region_agg = data.groupby("region").agg(
        total_sales=("sales", "sum"),
        total_profit=("profit", "sum"),
        total_quantity=("quantity", "sum"),
        order_count=("order_id", "nunique")
    ).reset_index()
    # Save to S3
    sales_by_region_agg.to_parquet(f"s3://{bucket_name}/processed/sales_by_region.parquet", index=False)
    logging.info(f"Sales by region metrics generated and uploaded to S3: s3://{bucket_name}/processed/sales_by_region.parquet")

def sales_by_category(data, bucket_name = BUCKET_NAME):
    logging.info("Generating sales by category...")
    sales_by_category_agg = data.groupby(["category", "sub_category"]).agg(
        total_sales=("sales", "sum"),
        total_profit=("profit", "sum"),
        avg_discount=("discount", "mean")
    ).reset_index()
    # Save to S3
    sales_by_category_agg.to_parquet(f"s3://{bucket_name}/processed/sales_by_category.parquet", index=False)
    logging.info(f"Sales by category metrics generated and uploaded to S3: s3://{bucket_name}/processed/sales_by_category.parquet")

# -----------------------------------------------------------------------
# Date-Based Analysis
# -----------------------------------------------------------------------
def monthly_sales(data, bucket_name = BUCKET_NAME):
    logging.info("Generating monthly sales metrics...")
    data["year"] = data["order_date"].dt.year
    data["month"] = data["order_date"].dt.month

    monthly_sales = data.groupby(["year", "month"]).agg(
        total_sales=("sales", "sum"),
        total_profit=("profit", "sum")
    ).reset_index()
    # Save to S3
    monthly_sales.to_parquet(f"s3://{bucket_name}/processed/monthly_sales.parquet", index=False)
    logging.info(f"Monthly sales metrics generated and uploaded to S3: s3://{bucket_name}/processed/monthly_sales.parquet")

# -----------------------------------------------------------------------
# Main function to run all steps
# -----------------------------------------------------------------------
def main():
    data = read_csv_s3(BUCKET_NAME, S3_RAW_UPLOAD_KEY)
    cleaned_data = data_cleaning(data)
    upload_cleaned_data(cleaned_data, BUCKET_NAME, S3_PROCESSED_UPLOAD_KEY)
    generate_summary_metrics(cleaned_data, BUCKET_NAME)
    sales_by_region(cleaned_data, BUCKET_NAME)
    sales_by_category(cleaned_data, BUCKET_NAME)
    monthly_sales(cleaned_data, BUCKET_NAME)


if __name__ == "__main__":
    main()