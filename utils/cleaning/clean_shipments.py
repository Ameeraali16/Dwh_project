import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/shipments/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/shipments/'

VALID_STATUSES = {'Delivered', 'Returned', 'In Transit', 'Processing', 'Delayed'}
VALID_SHIPMODES = {'Air', 'Sea', 'Road', 'Standard', 'Express'}

def clean_shipment_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_shipments_file', task_ids='download_shipments_csv')
    if not filename:
        raise FileNotFoundError("No filename found in XCom from download task")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    logging.info(f"Loading raw shipment data from {raw_file}")
    df = pd.read_csv(raw_file, dtype=str)

    # shipment_id and order_id must be positive integers
    for col in ['shipment_id', 'order_id']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[df[col].notnull() & (df[col] > 0)]
        df[col] = df[col].astype(int)

    # Clean ship_date
    df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
    df = df[df['ship_date'].notnull()]

    # Normalize and filter delivery_status
    df['delivery_status'] = df['delivery_status'].str.strip().str.title()
    df = df[df['delivery_status'].isin(VALID_STATUSES)]

    # Normalize shipmode and filter
    df['shipmode'] = df['shipmode'].str.strip().str.title().str.replace(' And ', ' / ')
    df = df[df['shipmode'].isin(VALID_SHIPMODES)]

    # Save cleaned file
    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned shipment data saved to {cleaned_file}")
