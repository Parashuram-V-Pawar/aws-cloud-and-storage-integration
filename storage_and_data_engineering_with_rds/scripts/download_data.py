## Download data from S3 bucket
import os
import logging
from botocore.exceptions import *
from config.s3_client_config import get_s3_client, BUCKET_NAME, LOCAL_DOWNLOAD_FILE, S3_UPLOAD_KEY

logging.basicConfig(level=logging.INFO)

# function to list files in S3 bucket
def list_files_in_bucket(bucket_name = BUCKET_NAME):
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket = bucket_name)
        if 'Contents' in response:
            logging.info(f"Files in bucket '{bucket_name}':")
            for obj in response['Contents']:
                logging.info(f" - {obj['Key']}")
        else:
            logging.info(f"No files found in bucket '{bucket_name}'.")
    except Exception as e:
        logging.error(f"Failed to list files: {e}")

# Function to download a files from S3 bucket
def download_file(local_download_file = LOCAL_DOWNLOAD_FILE, s3_key = S3_UPLOAD_KEY, bucket_name = BUCKET_NAME):
    s3 = get_s3_client()
    directory = os.path.dirname(local_download_file)

    # Checks if download directory exists, if not creates it
    if directory: 
        os.makedirs(directory, exist_ok=True)
    try:
        s3.download_file(
            bucket_name,
            s3_key,
            local_download_file
        )
        logging.info(f"Download successful: {local_download_file}")
        
    except ClientError as e:
        logging.error(f"S3 ClientError: {e}")

    except NoCredentialsError:
        logging.error("AWS credentials not available.")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    list_files_in_bucket()
    download_file()