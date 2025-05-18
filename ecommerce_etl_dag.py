import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from utils.s3_utils import check_for_new_file, download_file_from_s3, upload_cleaned_file_to_s3
from utils.snowflake_loader import load_to_snowflake
from utils.snowflake_loader import run_snowflake_merge



# Cleaning functions (same as before)
from utils.cleaning.clean_customers import clean_customer_data
from utils.cleaning.clean_orders import clean_order_data
from utils.cleaning.clean_payments import clean_payment_data
from utils.cleaning.clean_stores import clean_store_data
from utils.cleaning.clean_shipments import clean_shipment_data
from utils.cleaning.clean_inventory import clean_inventory_data
from utils.cleaning.clean_reviews import clean_reviews_data
from utils.cleaning.clean_returns import clean_returns_data
from utils.cleaning.clean_promotions import clean_promotions_data
from utils.cleaning.clean_order_items import clean_order_items_data
from utils.cleaning.clean_employees import clean_employees_data
from utils.cleaning.clean_customer_support import clean_customer_support_data
from utils.cleaning.clean_customer_loyalty import clean_customer_loyalty_data
from utils.cleaning.clean_products import clean_product_data
from utils.cleaning.clean_categories import clean_category_data

TABLE_CLEANING_FUNCS = {
    "customers": clean_customer_data,
    "orders": clean_order_data,
    "payments": clean_payment_data,
    "stores": clean_store_data,
    "shipments": clean_shipment_data,
    "inventory": clean_inventory_data,
    "reviews": clean_reviews_data,
    "returns": clean_returns_data,
    "promotions": clean_promotions_data,
    "order_items": clean_order_items_data,
    "employees": clean_employees_data,
    "customer_support": clean_customer_support_data,
    "customer_loyalty": clean_customer_loyalty_data,
    "products" : clean_product_data,
    "categories" : clean_category_data
}

MERGE_SQL_FILES = {
    "customers": "sql/dim_customer_merge.sql",
    "orders": "sql/dim_order_merge.sql",
    "products": "sql/dim_product_merge.sql",
    "returns": "sql/dim_return_merge.sql",
    "reviews": "sql/dim_review_merge.sql",
    "stores": "sql/dim_store_merge.sql",
    "date" :"sql/dim_date_insert.sql",
    "sales_returns": "sql/fact_sales_return.sql",
}

# rest of your DAG code unchanged
default_args = {
    'owner': 'ameera',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='ecommerce_full_etl',
    default_args=default_args,
    description='ETL for all ecommerce tables from S3 to Snowflake with merge steps',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'etl'],
) as dag:

    for table in TABLE_CLEANING_FUNCS.keys():
        check_task = PythonOperator(
            task_id=f'check_new_{table}_file',
            python_callable=check_for_new_file,
            op_kwargs={'table_name': table},
        )

        download_task = PythonOperator(
            task_id=f'download_{table}_csv',
            python_callable=download_file_from_s3,
            op_kwargs={'table_name': table},
            provide_context=True,
        )

        clean_task = PythonOperator(
            task_id=f'clean_{table}_data',
            python_callable=TABLE_CLEANING_FUNCS[table],
            provide_context=True,
        )

        upload_task = PythonOperator(
            task_id=f'upload_cleaned_{table}_to_s3',
            python_callable=upload_cleaned_file_to_s3,
            op_kwargs={'table_name': table},
        )

        load_task = PythonOperator(
            task_id=f'load_{table}_to_snowflake',
            python_callable=load_to_snowflake,
            op_kwargs={'table_name': table},
        )

       # Replace SnowflakeOperator merge task with PythonOperator
        if table in MERGE_SQL_FILES:
            merge_task = PythonOperator(
                task_id=f'merge_dim_{table}',
                python_callable=run_snowflake_merge,
                op_kwargs={'sql_file_path': os.path.join(os.path.dirname(__file__), MERGE_SQL_FILES[table])}
            )
            load_task >> merge_task

        # Set base dependencies for ingestion, cleaning, and loading
        check_task >> download_task >> clean_task >> upload_task >> load_task
