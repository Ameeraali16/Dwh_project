import pandas as pd
import os
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/promotions/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/promotions/'

def clean_promotions_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_promotions_file', task_ids='download_promotions_csv')
    if not filename:
        raise FileNotFoundError("No promotions file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean promotion_id
    df['promotion_id'] = df['promotion_id'].astype(str).str.extract(r'(\d+)')[0]
    df['promotion_id'] = pd.to_numeric(df['promotion_id'], errors='coerce')

    # Clean discount_percent
    df['discount_percent'] = pd.to_numeric(df['discount_percent'], errors='coerce')
    df = df[(df['discount_percent'] >= 0) & (df['discount_percent'] <= 100)]

    # Clean dates
    df['valid_from'] = pd.to_datetime(df['valid_from'], errors='coerce')
    df['valid_to'] = pd.to_datetime(df['valid_to'], errors='coerce')
    df = df[df['valid_from'].notnull() & df['valid_to'].notnull()]

    # Drop rows with missing IDs or promo_code
    df = df[df['promotion_id'].notnull() & df['promo_code'].notnull()]

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned promotions data saved to {cleaned_file}")
