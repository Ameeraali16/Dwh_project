import pandas as pd
import os
import re
import logging

LOCAL_RAW_PATH = '/tmp/airflow_raw/products/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/products/'

def clean_product_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    filename = ti.xcom_pull(key='latest_products_file', task_ids='download_products_csv')
    if not filename:
        raise FileNotFoundError("No filename found in XCom from download task")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    logging.info(f"Loading raw product data from {raw_file}")
    df = pd.read_csv(raw_file, dtype=str)

    # Clean product_id: extract numeric part
    def extract_numeric_id(pid):
        if pd.isna(pid):
            return None
        digits = re.findall(r'\d+', pid)
        return int(digits[0]) if digits else None

    df['product_id'] = df['product_id'].apply(extract_numeric_id)
    df = df.dropna(subset=['product_id'])

    # Clean product_name: drop if missing or numeric only
    def is_valid_name(name):
        if pd.isna(name) or name.strip() == '':
            return False
        return not name.strip().isdigit()

    df = df[df['product_name'].apply(is_valid_name)]
    df['product_name'] = df['product_name'].str.strip()

    # Clean category_id: keep only numeric values
    df = df[df['category_id'].fillna('').str.isdigit()]
    df['category_id'] = df['category_id'].astype(int)

    # Clean price: convert to float, replace non-numeric with 0.0
    def clean_price(val):
        try:
            return float(val)
        except:
            return 0.0

    df['price'] = df['price'].apply(clean_price)

    # Clean stock_quantity: force int, handle True/False, clip negatives to 0
    def clean_stock(val):
        if val in ['True', 'False']:
            return 0
        try:
            val_int = int(float(val))
            return max(val_int, 0)
        except:
            return 0

    df['stock_quantity'] = df['stock_quantity'].apply(clean_stock)

    # Final type casting
    df['product_id'] = df['product_id'].astype(int)

    # Save cleaned file
    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned product data saved to {cleaned_file}")
