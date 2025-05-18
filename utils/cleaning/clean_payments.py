# In utils/cleaning/clean_payments.py
import pandas as pd
import os
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/payments/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/payments/'

def clean_payment_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_payments_file', task_ids='download_payments_csv')
    if not filename:
        raise FileNotFoundError("No payments file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Rename the column to match Snowflake table
    df.rename(columns={'amount_paid': 'amount'}, inplace=True)

    # Convert and clean columns
    df['order_id'] = pd.to_numeric(df['order_id'].str.extract(r'(\d+)')[0], errors='coerce')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Drop rows with invalid or missing values
    df = df[df['order_id'].notnull() & df['payment_date'].notnull() & df['amount'].notnull()]
    df = df[df['amount'] > 0]

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"✅ Cleaned payments data saved to {cleaned_file}")
