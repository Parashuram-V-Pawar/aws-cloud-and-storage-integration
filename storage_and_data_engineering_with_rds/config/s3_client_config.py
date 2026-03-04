import os
import boto3
from dotenv import load_dotenv

# -----------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------------------
# AWS configuration
# -----------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# -----------------------------------------------------------------------
# client initialization functions
# -----------------------------------------------------------------------
def get_s3_client():
    session = boto3.Session(region_name=AWS_REGION)
    return session.client("s3")

# -----------------------------------------------------------------------
# File paths
# -----------------------------------------------------------------------
LOCAL_UPLOAD_FILE = os.getenv("LOCAL_UPLOAD_FILE")
S3_RAW_UPLOAD_KEY = os.getenv("S3_UPLOAD_KEY")
LOCAL_DOWNLOAD_FILE = os.getenv("LOCAL_DOWNLOAD_FILE")
S3_PROCESSED_UPLOAD_KEY = os.getenv("S3_PROCESSED_UPLOAD_KEY")