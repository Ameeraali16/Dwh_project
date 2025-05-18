import pandas as pd
import os
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/customer_support/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/customer_support/'

def clean_customer_support_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_customer_support_file', task_ids='download_customer_support_csv')
    if not filename:
        raise FileNotFoundError("No customer support file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean ticket_id and customer_id
    df['ticket_id'] = df['ticket_id'].astype(str).str.extract(r'(\d+)')[0]
    df['ticket_id'] = pd.to_numeric(df['ticket_id'], errors='coerce')

    df['customer_id'] = df['customer_id'].astype(str).str.extract(r'(\d+)')[0]
    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')

    # Normalize issue descriptions
    df['issue'] = df['issue'].fillna('').str.strip()
    df['issue'] = df['issue'].str.replace(r'(Billing issue)+', 'Billing issue', regex=True)
    df = df[df['issue'] != '']

    # Clean status
    valid_statuses = ['Open', 'In Progress', 'Closed', 'Resolved', 'Escalated']
    df['status'] = df['status'].astype(str).str.title()
    df = df[df['status'].isin(valid_statuses)]

    # Clean resolved_by
    df['resolved_by'] = pd.to_numeric(df['resolved_by'], errors='coerce')
    df = df[df['resolved_by'] > 0]

    df = df.dropna(subset=['ticket_id', 'customer_id', 'issue', 'status', 'resolved_by'])

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned customer support data saved to {cleaned_file}")
