from modules import s3_utils
from config import settings

def main():
    s3_client = s3_utils.get_s3_client(settings.AWS_REGION)
    
    # Upload
    s3_utils.upload_file(
        s3_client,
        settings.BUCKET_NAME,
        settings.LOCAL_UPLOAD_FILE,
        settings.S3_UPLOAD_KEY
    )

    # List files
    s3_utils.list_files(s3_client, settings.BUCKET_NAME, prefix='')

    # Download
    s3_utils.download_file(
        s3_client,
        settings.BUCKET_NAME,
        settings.S3_UPLOAD_KEY,
        settings.LOCAL_DOWNLOAD_FILE
    )

    # Verify integrity
    s3_utils.verify_file_integrity(
        s3_client,
        settings.BUCKET_NAME,
        settings.S3_UPLOAD_KEY,
        settings.LOCAL_DOWNLOAD_FILE
    )

if __name__ == "__main__":
    main()