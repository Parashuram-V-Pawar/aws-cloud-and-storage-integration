import os
import boto3

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "parashu-aws-lab-unique")

# client initialization functions
def get_s3_client():
    session = boto3.Session(region_name=AWS_REGION)
    return session.client("s3")

# File paths
LOCAL_UPLOAD_FILE = os.getenv("LOCAL_UPLOAD_FILE", "data/superstore_sales_6000_records.csv")
S3_RAW_UPLOAD_KEY = os.getenv("S3_UPLOAD_KEY", "raw/superstore_sales_6000_records.csv")
LOCAL_DOWNLOAD_FILE = os.getenv("LOCAL_DOWNLOAD_FILE", "download/superstore_sales_6000_records.csv")
S3_PROCESSED_UPLOAD_KEY = os.getenv("S3_PROCESSED_UPLOAD_KEY", "processed/cleaned_superstore_sales_6000_records.csv")