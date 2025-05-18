import boto3
import os
import logging

# AWS S3 client
s3 = boto3.client('s3')

# Config - set your bucket here
BUCKET_NAME = 'ecommerce-dwh-bucket'  # <-- Replace with your actual bucket name

# Base prefixes for raw and staging folders in S3
RAW_DATA_BASE_PREFIX = 'raw_data'
STAGING_BASE_PREFIX = 'staging'

# Base local paths for Airflow worker to store files temporarily
LOCAL_BASE_RAW = '/tmp/airflow_raw'
LOCAL_BASE_CLEANED = '/tmp/airflow_cleaned'

def get_s3_raw_prefix(table_name):
    return f"{RAW_DATA_BASE_PREFIX}/{table_name}/"

def get_s3_staging_prefix(table_name):
    return f"{STAGING_BASE_PREFIX}/{table_name}/"

def get_local_raw_path(table_name):
    path = os.path.join(LOCAL_BASE_RAW, table_name)
    os.makedirs(path, exist_ok=True)
    return path

def get_local_cleaned_path(table_name):
    path = os.path.join(LOCAL_BASE_CLEANED, table_name)
    os.makedirs(path, exist_ok=True)
    return path

def check_for_new_file(table_name):
    prefix = get_s3_raw_prefix(table_name)
    logging.info(f"Checking for new files in S3 at {prefix}")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' not in response:
        logging.info(f"No files found in {prefix}")
        return None

    # Filter only CSV files
    files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    if not files:
        logging.info(f"No CSV files found in {prefix}")
        return None

    latest_obj = max(files, key=lambda obj: obj['LastModified'])
    latest_key = latest_obj['Key']
    logging.info(f"Latest file found: {latest_key}")
    return latest_key

def download_file_from_s3(table_name, ti=None):
    latest_key = check_for_new_file(table_name)
    if not latest_key:
        raise FileNotFoundError(f"No new files found in raw_data/{table_name}/")

    filename = os.path.basename(latest_key)
    local_path = os.path.join(get_local_raw_path(table_name), filename)
    logging.info(f"Downloading {latest_key} to local path {local_path}")

    s3.download_file(BUCKET_NAME, latest_key, local_path)

    # Push filename to XCom for downstream tasks
    if ti:
        ti.xcom_push(key=f'latest_{table_name}_file', value=filename)

def upload_cleaned_file_to_s3(table_name, ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) is required for XCom")

    filename = ti.xcom_pull(key=f'latest_{table_name}_file', task_ids=f'download_{table_name}_csv')
    if not filename:
        raise FileNotFoundError(f"No filename found in XCom from download task for {table_name}")

    local_cleaned_file = os.path.join(get_local_cleaned_path(table_name), filename)
    s3_key = os.path.join(get_s3_staging_prefix(table_name), filename)
    logging.info(f"Uploading cleaned file {local_cleaned_file} to S3 at {s3_key}")

    s3.upload_file(local_cleaned_file, BUCKET_NAME, s3_key)
