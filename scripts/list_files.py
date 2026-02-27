from modules import s3_utils
from config import settings

if __name__ == "__main__":
    s3_client = s3_utils.get_s3_client(settings.AWS_REGION)
    s3_utils.list_files(s3_client, settings.BUCKET_NAME, prefix='')