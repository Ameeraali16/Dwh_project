import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/stores/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/stores/'

def clean_store_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_stores_file', task_ids='download_stores_csv')
    if not filename:
        raise FileNotFoundError("No filename found in XCom from download task")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    logging.info(f"Loading raw store data from {raw_file}")
    df = pd.read_csv(raw_file, dtype=str)

    # Clean store_id: extract digits and drop if not valid
    def clean_store_id(val):
        if pd.isna(val):
            return None
        digits = re.findall(r'\d+', val)
        if digits:
            num = int(digits[0])
            return num if num >= 0 else None
        return None

    df['store_id'] = df['store_id'].apply(clean_store_id)
    df = df.dropna(subset=['store_id'])
    df['store_id'] = df['store_id'].astype(int)

    # Drop store_name if missing or numeric-only
    def valid_name(val):
        if pd.isna(val) or val.strip() == '':
            return False
        return not re.fullmatch(r'\d+', val.strip())

    df = df[df['store_name'].apply(valid_name)]

    # Drop invalid addresses
    def valid_address(val):
        if pd.isna(val) or val.strip() == '':
            return False
        return not re.fullmatch(r'\d+', val.strip())

    df = df[df['address'].apply(valid_address)]

    # Clean city: remove rows that are None, numbers, or overly repeated words
    def valid_city(val):
        if pd.isna(val) or val.strip() == '':
            return False
        if re.fullmatch(r'\d+', val.strip()):
            return False
        if len(set(val.strip().lower().split())) == 1:  # repeated word
            return False
        return True

    df = df[df['city'].apply(valid_city)]

    # Clean manager: drop rows with None, garbage, or numeric-only
    def valid_manager(val):
        if pd.isna(val) or val.strip() == '':
            return False
        if '???' in val or re.fullmatch(r'\d+', val.strip()):
            return False
        return True

    df = df[df['manager'].apply(valid_manager)]

    # Save cleaned file
    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned store data saved to {cleaned_file}")
