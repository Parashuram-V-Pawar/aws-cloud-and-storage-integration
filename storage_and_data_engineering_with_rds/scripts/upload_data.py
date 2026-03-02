## Upload RAW data to S3 bucket
import os
import logging
from botocore.exceptions import *
from config.s3_client_config import get_s3_client, BUCKET_NAME, LOCAL_UPLOAD_FILE, S3_UPLOAD_KEY

logging.basicConfig(level=logging.INFO)

def upload_file():
    s3 = get_s3_client()

    if not os.path.exists(LOCAL_UPLOAD_FILE):
        logging.error("Local file does not exist.")
        return
    try:
        s3.upload_file(
            LOCAL_UPLOAD_FILE,
            BUCKET_NAME,
            S3_UPLOAD_KEY
        )
        logging.info(f"Upload successful: s3://{BUCKET_NAME}/{S3_UPLOAD_KEY}")
    except FileNotFoundError:
        logging.error("The specified file was not found.")
    except NoCredentialsError:
        logging.error("AWS credentials not available.")
    except Exception as e:
        logging.error(f"Upload failed: {e}")

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
        
if __name__ == "__main__":
    upload_file()
    list_files_in_bucket()