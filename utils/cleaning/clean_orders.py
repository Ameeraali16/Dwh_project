import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/orders/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/orders/'

def clean_order_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_orders_file', task_ids='download_orders_csv')
    if not filename:
        raise FileNotFoundError("No filename found in XCom from download task")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    logging.info(f"Loading raw orders data from {raw_file}")
    df = pd.read_csv(raw_file, dtype=str)

    # Clean order_id: keep only numeric portion
    def extract_order_id(val):
        if pd.isna(val):
            return None
        digits = re.findall(r'\d+', val)
        return int(digits[0]) if digits else None

    df['order_id'] = df['order_id'].apply(extract_order_id)

       # Clean customer_id: keep only numeric portion
    def extract_customer_id(val):
        if pd.isna(val):
            return None
        digits = re.findall(r'\d+', val)
        return int(digits[0]) if digits else None

    df['customer_id'] = df['customer_id'].apply(extract_customer_id)
    df = df.dropna(subset=['order_id', 'customer_id'])
    df['customer_id'] = df['customer_id'].astype(int)


    # Clean order_date: parse to datetime, format as YYYY-MM-DD
    def parse_date(val):
        try:
            return pd.to_datetime(val).strftime('%Y-%m-%d')
        except:
            return None

    df['order_date'] = df['order_date'].apply(parse_date)
    df = df.dropna(subset=['order_date'])

    # Clean shipping_address: remove rows that are just numbers or nonsense
    def is_valid_address(val):
        if pd.isna(val) or val.strip() == '':
            return False
        if re.fullmatch(r'\d+', val):  # only digits
            return False
        if '???' in val or val.strip() in ['???', '???!!!']:
            return False
        return True

    df = df[df['shipping_address'].apply(is_valid_address)]

    # Clean total_amount: convert to float, default to 0.0 if invalid
    def clean_amount(val):
        try:
            return float(val)
        except:
            return 0.0

    df['total_amount'] = df['total_amount'].apply(clean_amount)

    # Final type casting
    df['order_id'] = df['order_id'].astype(int)

    # Save cleaned file
    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned order data saved to {cleaned_file}")
