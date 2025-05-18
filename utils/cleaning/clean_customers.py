import pandas as pd
import os
import re
import logging

# Paths for raw and cleaned data
LOCAL_RAW_PATH = '/tmp/airflow_raw/customers/'
LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/customers/'

# Regex for validating email addresses
EMAIL_REGEX = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"

def is_valid_email(email):
    if pd.isna(email):
        return False
    return re.match(EMAIL_REGEX, email) is not None

def clean_phone(phone):
    """
    Cleans and validates phone numbers to a standard 10-digit US format.
    Steps:
    - Remove all non-digit characters (including '+').
    - If more than 10 digits, keep only the first 10 digits.
    - If fewer than 10 digits, mark as 'Unknown'.
    """
    if pd.isna(phone):
        return 'Unknown'
    
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', phone)
    
    # If length more than 10, truncate to 10 digits (assuming US number)
    if len(digits_only) > 10:
        digits_only = digits_only[:10]
    
    # If exactly 10 digits, format as (XXX) XXX-XXXX or just return digits
    if len(digits_only) == 10:
        # Optional: format phone nicely like (123) 456-7890
        formatted = f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        return formatted
    
    return 'Unknown'


def clean_customer_data(ti=None):
    if ti is None:
        raise ValueError("Task instance (ti) required for XCom")

    # Get latest customer file name from XCom
    filename = ti.xcom_pull(key='latest_customers_file', task_ids='download_customers_csv')
    if not filename:
        raise FileNotFoundError("No filename found in XCom from download task")

    raw_file = os.path.join(LOCAL_RAW_PATH, filename)
    cleaned_file = os.path.join(LOCAL_CLEANED_PATH, filename)

    logging.info(f"Loading raw customer data from {raw_file}")
    df = pd.read_csv(raw_file, dtype=str)  # Read all columns as string

    # Clean customer_id: extract digits, convert to int, drop invalids
    def clean_customer_id(val):
        if pd.isna(val):
            return None
        digits = re.findall(r'\d+', val)
        return int(digits[0]) if digits else None

    df['customer_id'] = df['customer_id'].apply(clean_customer_id)

    # Drop rows with missing customer_id or name
    df = df.dropna(subset=['customer_id', 'name'])

    # Clean and validate emails
    df['email'] = df['email'].str.lower()
    df = df[df['email'].apply(is_valid_email)]

    # Clean phone numbers
    df['phone'] = df['phone'].apply(clean_phone)

    # Fill missing address and city with 'Unknown'
    df['address'] = df['address'].fillna('Unknown')
    df['city'] = df['city'].fillna('Unknown')

    # Save cleaned data
    os.makedirs(LOCAL_CLEANED_PATH, exist_ok=True)
    df.to_csv(cleaned_file, index=False)
    logging.info(f"Cleaned customer data saved to {cleaned_file}")
