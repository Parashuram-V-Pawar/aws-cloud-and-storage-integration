## Download data from S3 bucket
import os
import logging
from botocore.exceptions import *
from config.s3_client_config import *

logging.basicConfig(level=logging.INFO)

# function to list files in S3 bucket
def list_files_in_bucket():
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        if 'Contents' in response:
            logging.info(f"Files in bucket '{BUCKET_NAME}':")
            for obj in response['Contents']:
                logging.info(f" - {obj['Key']}")
        else:
            logging.info(f"No files found in bucket '{BUCKET_NAME}'.")
    except Exception as e:
        logging.error(f"Failed to list files: {e}")

# Function to download a files from S3 bucket
def download_file():
    s3 = get_s3_client()
    directory = os.path.dirname(LOCAL_DOWNLOAD_FILE)

    # Checks if download directory exists, if not creates it
    if directory: 
        os.makedirs(directory, exist_ok=True)
    try:
        s3.download_file(
            BUCKET_NAME,
            S3_UPLOAD_KEY,
            LOCAL_DOWNLOAD_FILE
        )
        logging.info(f"Download successful: {LOCAL_DOWNLOAD_FILE}")
        
    except ClientError as e:
        logging.error(f"S3 ClientError: {e}")

    except NoCredentialsError:
        logging.error("AWS credentials not available.")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    list_files_in_bucket()
    download_file()