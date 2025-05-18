import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/reviews/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/reviews/'

def clean_reviews_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_reviews_file', task_ids='download_reviews_csv')
    if not filename:
        raise FileNotFoundError("No reviews file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean IDs
    for col in ['review_id', 'product_id', 'customer_id']:
        df[col] = df[col].astype(str).str.extract(r'(\d+)')[0]
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['review_id', 'product_id', 'customer_id'])
    df = df[(df['review_id'] > 0) & (df['product_id'] > 0) & (df['customer_id'] > 0)]

    # Clean rating
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df = df[df['rating'].isin([1, 2, 3, 4, 5])]

    # Clean review_text
    df['review_text'] = df['review_text'].astype(str)
    df = df[~df['review_text'].str.contains(r'\?{3,}|None', na=False)]

    # Parse and filter valid dates
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    df = df[df['review_date'].notnull()]

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned reviews data saved to {cleaned_file}")
