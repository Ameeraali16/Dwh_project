import pandas as pd
import os
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/employees/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/employees/'

def clean_employees_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_employees_file', task_ids='download_employees_csv')
    if not filename:
        raise FileNotFoundError("No employee file found in XCom")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    df = pd.read_csv(raw_file, dtype=str)

    # Clean employee_id
    df['employee_id'] = df['employee_id'].astype(str).str.extract(r'(\d+)')[0]
    df['employee_id'] = pd.to_numeric(df['employee_id'], errors='coerce')

    # Clean store_id
    df['store_id'] = pd.to_numeric(df['store_id'], errors='coerce')

    # Clean name and role
    df['name'] = df['name'].str.title().str.strip()
    df['role'] = df['role'].str.title().str.strip()

    # Clean joining_date
    df['joining_date'] = pd.to_datetime(df['joining_date'], errors='coerce')

    df = df.dropna(subset=['employee_id', 'store_id', 'name', 'role', 'joining_date'])

    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned employees data saved to {cleaned_file}")
