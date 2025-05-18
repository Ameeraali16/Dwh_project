import snowflake.connector
import os
import logging

# Snowflake connection config
SNOWFLAKE_ACCOUNT = 'EXAZTZU-DU83523'
SNOWFLAKE_USER = 'AMEERAALI'
SNOWFLAKE_PASSWORD = 'Ameeraali1623#'
SNOWFLAKE_WAREHOUSE = 'EC_WAREHOUSE'
SNOWFLAKE_DATABASE = 'ECOMMERCE_DB'
SNOWFLAKE_SCHEMA = 'ECOMMERCE_SCHEMA'

BASE_LOCAL_CLEANED_PATH = '/tmp/airflow_cleaned/'  # Folder contains subfolders per table

# Define explicit column mappings if needed (e.g., skip autoincrement fields)
TABLE_COLUMN_OVERRIDES = {
    'order_items': '(ORDER_ID, PRODUCT_ID, QUANTITY, ITEM_PRICE)',
    # Add more here if needed
}


def run_snowflake_merge(sql_file_path: str):
    """
    Connects to Snowflake, reads a SQL merge file, and executes it.
    """
    logging.info(f"Running Snowflake merge SQL from {sql_file_path}")
    try:
        with open(sql_file_path, 'r') as f:
            merge_sql = f.read()

        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema='STAR_SCHEMA'
        )
        cs = ctx.cursor()
        cs.execute(merge_sql)
        ctx.commit()
        logging.info(f"✅ Successfully ran merge SQL: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Error running merge SQL {sql_file_path}", exc_info=True)
        raise
    finally:
        cs.close()
        ctx.close()

def truncate_table(cursor, table_name):
    try:
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name.upper()}")
        logging.info(f"🧹 Truncated table: {table_name.upper()}")
    except Exception as e:
        logging.error(f"❌ Failed to truncate table {table_name.upper()}", exc_info=True)
        raise e


def load_to_snowflake(table_name: str, ti=None):
    """
    Generic loader function for any table with support for skipping auto-increment columns.
    """
    if ti is None:
        raise ValueError("Task instance (ti) is required for XCom")

    # Get cleaned filename from XCom
    xcom_key = f"latest_{table_name}_file"
    filename = ti.xcom_pull(key=xcom_key, task_ids=f'download_{table_name}_csv')

    if not filename:
        raise FileNotFoundError(f"No filename found in XCom for table {table_name}")

    local_cleaned_path = os.path.join(BASE_LOCAL_CLEANED_PATH, table_name)
    cleaned_file_path = os.path.join(local_cleaned_path, filename)

    if not os.path.isfile(cleaned_file_path):
        raise FileNotFoundError(f"Cleaned CSV file not found: {cleaned_file_path}")

    logging.info(f"Connecting to Snowflake to load {table_name} from {cleaned_file_path}")

    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cs = ctx.cursor()

        truncate_table(cs, table_name)


        # Upload to internal stage
        stage_name = f'@%{table_name.upper()}'
        put_cmd = f"PUT file://{cleaned_file_path} {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        logging.info(f"Uploading to Snowflake stage: {put_cmd}")
        cs.execute(put_cmd)

        # Determine if we need to override columns (e.g., skip auto-increment ones)
        column_clause = TABLE_COLUMN_OVERRIDES.get(table_name, '')

        copy_cmd = f"""
            COPY INTO {table_name.upper()} {column_clause}
            FROM {stage_name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
            )
            ON_ERROR = 'CONTINUE';
        """
        logging.info(f"Running COPY INTO for {table_name.upper()}")
        cs.execute(copy_cmd)

        ctx.commit()
        logging.info(f"✅ Successfully loaded data into {table_name.upper()}")

    except Exception as e:
        logging.error(f"❌ Failed to load data into Snowflake table {table_name.upper()}", exc_info=True)
        raise e

    finally:
        cs.close()
        ctx.close()
