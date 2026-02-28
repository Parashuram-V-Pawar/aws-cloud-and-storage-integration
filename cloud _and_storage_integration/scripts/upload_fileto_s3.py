# Task A: Local to S3 Upload
# Write Python programs using Boto3 to:
# Upload a small CSV file from local system to S3
# Upload a large file (simulate large size if required)
# Handle:
# File not found errors
# Invalid credentials
# Bucket not available scenarios

import boto3
import botocore
import os

def upload_file_to_s3(file_path, bucket_name, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Error: Bucket '{bucket_name}' does not exist.")
        else:
            print(f"Client error: {e}")
    except botocore.exceptions.NoCredentialsError:
        print("Error: Invalid AWS credentials.")
    except Exception as e:
        print(f"Error uploading file: {e}") 

if __name__ == "__main__":
    file_path = 'data/day_wise.csv'  
    bucket_name = 'parashu-aws-lab-unique'  
    upload_file_to_s3(file_path, bucket_name)