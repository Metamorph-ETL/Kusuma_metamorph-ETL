from airflow.decorators import task
from airflow.exceptions import AirflowException
from pyspark.sql.functions import col
import logging

from transform_utils import (
    init_spark,
    APIClient,
    load_data_task_from_df,  
    DuplicateValidator
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("ETL_logger")

def ingest_data(endpoint, column_renames, target_table, key_columns, auth=False):
    try:
        log.info(f"Initializing Spark session for endpoint: {endpoint}...")
        spark = init_spark()

        log.info(f"Fetching data from API endpoint: {endpoint}...")
        client = APIClient()
        response = client.fetch_data(endpoint, auth=auth)

        data = response.get("data")
        if not data or not isinstance(data, list):
            raise AirflowException(f"No valid data received from {endpoint}.")

        log.info(f"Received {len(data)} records from {endpoint}.")

        df = spark.createDataFrame(data)
        for old_col, new_col in column_renames.items():
            df = df.withColumnRenamed(old_col, new_col)

        df_tgt = df.select(*[col(new_col) for new_col in column_renames.values()])

        DuplicateValidator.validate_duplicates(df_tgt, key_columns=key_columns)

        log.info(f"Loading data into PostgreSQL table {target_table}...")
        load_data_task_from_df(df_tgt, target_table)  

        log.info(f"{endpoint} ETL process completed successfully.")
        return f"{endpoint} ETL process completed successfully."

    except Exception as e:
        log.error(f"ETL failed for {endpoint}: {str(e)}")
        raise AirflowException(f"ETL failed for {endpoint}: {str(e)}")

    finally:
        if 'spark' in locals():
            spark.stop()
            log.info(f"Spark session for {endpoint} stopped.")

@task()
def m_ingest_data_into_suppliers():
    return ingest_data(
        endpoint="v1/suppliers",
        column_renames={
            "supplier_id": "SUPPLIER_ID",
            "supplier_name": "SUPPLIER_NAME",
            "contact_details": "CONTACT_DETAILS",
            "region": "REGION"
        },
        target_table="raw.suppliers",
        key_columns=["SUPPLIER_ID"]
    )

@task()
def m_ingest_data_into_products():
    return ingest_data(
        endpoint="v1/products",
        column_renames={
            "product_id": "PRODUCT_ID",
            "product_name": "PRODUCT_NAME",
            "category": "CATEGORY",
            "price": "PRICE",
            "stock_quantity": "STOCK_QUANTITY",
            "reorder_level": "REORDER_LEVEL",
            "supplier_id": "SUPPLIER_ID"
        },
        target_table="raw.products",
        key_columns=["PRODUCT_ID"]
    )

@task()
def m_ingest_data_into_customers():
    return ingest_data(
        endpoint="v1/customers",
        column_renames={
            "customer_id": "CUSTOMER_ID",
            "name": "NAME",
            "city": "CITY",
            "email": "EMAIL",
            "phone_number": "PHONE_NUMBER"
        },
        target_table="raw.customers",
        key_columns=["CUSTOMER_ID"],
        auth=True
    )
