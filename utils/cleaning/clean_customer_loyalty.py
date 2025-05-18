import pandas as pd
import os
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/customer_loyalty/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/customer_loyalty/'

def clean_customer_loyalty_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_customer_loyalty_file', task_ids='download_customer_loyalty_csv')
    if not filename:
        raise FileNotFoundError("No customer loyalty file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean loyalty_id and customer_id
    df['loyalty_id'] = pd.to_numeric(df['loyalty_id'], errors='coerce')
    df['customer_id'] = pd.to_numeric(df['customer_id'].astype(str).str.extract(r'(\d+)')[0], errors='coerce')

    # Clean loyalty_tier
    valid_tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
    df['loyalty_tier'] = df['loyalty_tier'].str.title().str.strip()
    df = df[df['loyalty_tier'].isin(valid_tiers)]

    # Clean points_earned
    df['points_earned'] = pd.to_numeric(df['points_earned'], errors='coerce')
    df = df[df['points_earned'].notnull()]

    # Standardize is_active
    df['is_active'] = df['is_active'].astype(str).str.strip().str.lower()
    df['is_active'] = df['is_active'].map({
        'true': True,
        'false': False,
        'yes': True,
        'no': False
    })
    df = df[df['is_active'].notnull()]

    # Final cleanup
    df = df.dropna(subset=['loyalty_id', 'customer_id', 'loyalty_tier'])

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned customer loyalty data saved to {cleaned_file}")
