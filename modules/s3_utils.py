import boto3
import os
import botocore
import logging
import hashlib

logging.basicConfig(level=logging.INFO)

def get_s3_client(region_name):
    return boto3.client("s3", region_name=region_name)

def upload_file(s3_client, bucket_name, file_path, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"{file_path} does not exist")
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
    except FileNotFoundError as e:
        logging.error(e)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logging.error(f"Bucket '{bucket_name}' does not exist.")
        else:
            logging.error(f"Client error: {e}")
    except botocore.exceptions.NoCredentialsError:
        logging.error("Invalid AWS credentials.")
    except Exception as e:
        logging.error(f"Error uploading file: {e}")

def download_file(s3_client, bucket_name, s3_key, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        s3_client.download_file(bucket_name, s3_key, local_path)
        logging.info(f"File downloaded successfully to {local_path}")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error downloading file: {e}")

def verify_file_integrity(s3_client, bucket_name, s3_key, local_path):
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        expected_size = response['ContentLength']

        if os.path.exists(local_path):
            actual_size = os.path.getsize(local_path)
            if actual_size == expected_size:
                logging.info("File integrity verified: Size matches S3 object.")
            else:
                logging.warning(f"Integrity failed: S3 size {expected_size}, local size {actual_size}")
        else:
            logging.warning("File does not exist locally.")
    except Exception as e:
        logging.error("Error verifying integrity: %s", e)

def list_files(s3_client, bucket_name, prefix=''):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            logging.info(f"Files in bucket '{bucket_name}' with prefix '{prefix}':")
            for obj in response['Contents']:
                logging.info(obj['Key'])
            return [obj['Key'] for obj in response['Contents']]
        else:
            logging.info(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
            return []
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error listing files: {e}")
        return []