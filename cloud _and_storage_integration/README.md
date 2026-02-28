# AWS Cloud & Storage Integration Using Python

----
## AWS Services Used
- VPC: To create private, isolated network fro our instance.
- EC2: For optional execution environment.
- S3: For file storage.
- IAM: For access management.

## Script Flow
1.  Task 3:
    1. `upload_fileto_s3.py` - Upload a local CSV to S3.
    2. `download_filefrom_s3.py` - Download from S3 and verify integrity and list files in S3.

2. Task 4:
    1. `settings.py` - Configures the variables.
    2. `s3_utils.py` - Upload a file to S3, download a file from S3, list files and verify integrity.
    3. `run_all.py` - Controller program to execute the project sequentially. 
    4. `list_files.py` - List all files in S3 bucket/prefix

----
## Assumptions
- Bucket exists and is in region `ap-south-1`
- Sample CSV located at `data/day_wise.csv`
- AWS credentials configured via environment variables(local machine) or IAM role(EC2 Instance)
----