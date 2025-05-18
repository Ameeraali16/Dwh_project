import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/categories/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/categories/'

def is_valid_category_name(name):
    if pd.isna(name):
        return False
    if name.strip().isdigit():
        return False  # Reject names that are just numbers
    return True

def clean_category_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    # Pull file name from XCom pushed by download task
    filename = ti.xcom_pull(key='latest_categories_file', task_ids='download_categories_csv')
    if not filename:
        raise FileNotFoundError("No filename found in XCom for categories")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    logging.info(f"Loading raw category data from {raw_file}")
    df = pd.read_csv(raw_file, dtype=str)

    # Clean category_id: keep only rows with numeric IDs
    df = df[df['category_id'].fillna('').str.isdigit()]
    df['category_id'] = df['category_id'].astype(int)

    # Clean category_name: drop if None or numeric-only
    df = df[df['category_name'].apply(is_valid_category_name)]
    df['category_name'] = df['category_name'].str.strip()

    # Save cleaned file
    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned category data saved to {cleaned_file}")
