import pandas as pd
import os
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/order_items/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/order_items/'

def clean_order_items_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_order_items_file', task_ids='download_order_items_csv')
    if not filename:
        raise FileNotFoundError("No order items file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean order_id and product_id
    df['order_id'] = df['order_id'].astype(str).str.extract(r'(\d+)')[0]
    df['product_id'] = df['product_id'].astype(str).str.extract(r'(\d+)')[0]

    df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce')
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')

    # Clean quantity and item_price
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['item_price'] = pd.to_numeric(df['item_price'], errors='coerce')

    df = df[df['order_id'].notnull() & df['product_id'].notnull()]
    df = df[(df['quantity'] > 0) & (df['item_price'] > 0)]

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned order items data saved to {cleaned_file}")
