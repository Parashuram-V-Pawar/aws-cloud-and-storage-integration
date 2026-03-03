import pandas as pd
import logging
from config.s3_client_config import *

logging.basicConfig(level=logging.INFO)
s3 = get_s3_client()

def read_csv_s3(bucket = BUCKET_NAME, s3_key = S3_RAW_UPLOAD_KEY):
    logging.info("Reading data from s3....")
    s3_path = f"s3://{bucket}/{s3_key}"
    data = pd.read_csv(s3_path)
    logging.info("Read successful....")
    return data

def data_cleaning(data):
    logging.info("Data cleaning started....")
    data = data.drop(['row_id'], axis=1)
    data = data.drop_duplicates()
    data.dropna(subset = ['order_id','order_date','ship_date'], inplace=True)
    fill_values = {
        'customer_name' : 'Undefined',
        'segment' : 'Undefined',
        'region' : 'Undefined',
        'country' : 'Undefined',
        'category' : 'Undefined',
        'sub_category' : 'Undefined',
        'sales' : data['sales'].mean(),
        'quantity' : 0,
        'discount' : 0,
        'profit' : 0
    }
    data.fillna(value=fill_values, inplace=True)
    logging.info("Data cleaning Finished....")
    return data

def upload_cleaned_data(data, bucket_name = BUCKET_NAME, s3_key = S3_PROCESSED_UPLOAD_KEY):
    logging.info("Uploading cleaned data to S3...")
    data.to_csv(f"s3://{bucket_name}/{s3_key}")
    logging.info(f"Upload successful: s3://{bucket_name}/{s3_key}")

data = read_csv_s3(BUCKET_NAME,S3_RAW_UPLOAD_KEY)
cleaned_data = data_cleaning(data)
upload_cleaned_data(data,BUCKET_NAME,S3_PROCESSED_UPLOAD_KEY)
cleaned_data.head()