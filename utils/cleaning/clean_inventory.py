import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/inventory/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/inventory/'

def clean_inventory_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_inventory_file', task_ids='download_inventory_csv')
    if not filename:
        raise FileNotFoundError("No inventory file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Drop rows with missing product_id or store_id
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
    df['store_id'] = pd.to_numeric(df['store_id'], errors='coerce')
    df['quantity_available'] = pd.to_numeric(df['quantity_available'], errors='coerce')

    df = df[df['product_id'].notnull() & df['store_id'].notnull()]
    df = df[df['quantity_available'].notnull()]

    # Clean inventory_id
    df['inventory_id'] = pd.to_numeric(df['inventory_id'].str.extract(r'(\d+)')[0], errors='coerce')
    df = df[df['inventory_id'].notnull()]
    df['inventory_id'] = df['inventory_id'].astype(int)

    # Parse last_restocked date
    df['last_restocked'] = pd.to_datetime(df['last_restocked'], errors='coerce')
    df = df[df['last_restocked'].notnull()]

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned inventory data saved to {cleaned_file}")
