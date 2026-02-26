import os

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "parashu-aws-lab-unique")

# File paths
LOCAL_UPLOAD_FILE = os.getenv("LOCAL_UPLOAD_FILE", "data/day_wise.csv")
S3_UPLOAD_KEY = os.getenv("S3_UPLOAD_KEY", "day_wise.csv")
LOCAL_DOWNLOAD_FILE = os.getenv("LOCAL_DOWNLOAD_FILE", "download/day_wise.csv")