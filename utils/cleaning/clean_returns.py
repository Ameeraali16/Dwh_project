import pandas as pd
import os
import re
import logging
from datetime import datetime

LOCAL_RAW_PATH = '/tmp/airflow_raw/returns/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/returns/'

def clean_returns_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_returns_file', task_ids='download_returns_csv')
    if not filename:
        raise FileNotFoundError("No returns file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean numeric IDs
    for col in ['return_id', 'order_id', 'product_id']:
        df[col] = df[col].astype(str).str.extract(r'(\d+)')[0]
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['return_id', 'order_id', 'product_id'])

    # Clean return_date
    df['return_date'] = df['return_date'].replace({'Yesterday': pd.Timestamp.now().strftime('%Y-%m-%d')})
    df['return_date'] = pd.to_datetime(df['return_date'], errors='coerce')
    df = df[df['return_date'].notnull()]

    # Refund amount
    df['refund_amount'] = df['refund_amount'].replace('[\$,]', '', regex=True)
    df['refund_amount'] = pd.to_numeric(df['refund_amount'], errors='coerce')
    df = df[df['refund_amount'].notnull() & (df['refund_amount'] >= 0)]

    # Drop rows missing return_reason
    df = df[df['return_reason'].notnull() & (df['return_reason'].astype(str).str.len() > 3)]

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned returns data saved to {cleaned_file}")
