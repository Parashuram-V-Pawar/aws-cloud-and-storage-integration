# Task B: S3 to Local Download
# Write Python programs to:
# Download files from S3 to local system
# Verify file integrity after download
# List all files in a given S3 bucket/prefix

#------------------Importing necessary libraries------------------#
import boto3
import os
import botocore

#------------------Functions for S3 operations------------------#
# Function to download a file from S3 to local system
def download_file_from_s3(bucket_name, s3_key, local_path):
    s3 = boto3.client('s3')

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        s3.download_file(bucket_name, s3_key, local_path)
        print(f"File downloaded successfully to {local_path}")
    except botocore.exceptions.ClientError as e:
        print(f"Error downloading file: {e}")

# Function to verify file integrity by comparing sizes
def verify_file_integrity(bucket_name, s3_key, local_path):
    s3 = boto3.client('s3')

    try:
        response = s3.head_object(Bucket=bucket_name, Key=s3_key)
        expected_size = response['ContentLength']

        if os.path.exists(local_path):
            actual_size = os.path.getsize(local_path)

            if actual_size == expected_size:
                print("File integrity verified: Size matches S3 object.")
            else:
                print(f"Integrity failed: S3 size {expected_size}, local size {actual_size}")
        else:
            print("File does not exist locally.")

    except Exception as e:
        print("Error verifying integrity:", e)

# Function to list all files in a given S3 bucket
def list_files_in_s3_bucket(bucket_name, prefix=''):
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            print(f"Files in bucket '{bucket_name}' with prefix '{prefix}':")
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    except botocore.exceptions.ClientError as e:
        print(f"Error listing files: {e}")

#------------------Main function to execute the tasks------------------#
def main():
    bucket_name = 'parashu-aws-lab-unique'
    s3_key = 'day_wise.csv'
    local_path = 'download/day_wise.csv'
    download_file_from_s3(bucket_name, s3_key, local_path)
    verify_file_integrity(bucket_name, s3_key, local_path)
    list_files_in_s3_bucket(bucket_name, prefix='')

# Entry point of the script
if __name__ == "__main__":
    main()