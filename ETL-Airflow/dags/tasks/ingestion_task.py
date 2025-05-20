from airflow.decorators import task
from airflow.exceptions import AirflowException
from transform_utils import init_spark, APIClient, load_to_postgres, DuplicateValidator
import logging
from pyspark.sql.functions import col

log = logging.getLogger(__name__)

def ingest_data(endpoint, column_renames, target_table, key_columns, auth=False):

    #Generic ETL ingestion function.
    try:
        log.info(f"Initializing Spark session for endpoint: {endpoint}...")
        spark = init_spark()

        log.info(f"Fetching data from API endpoint: {endpoint}...")
        client = APIClient()
        response = client.fetch_data(endpoint, auth=auth)

        data = response.get("data")
        if not data:
            raise AirflowException(f"No data received or missing 'data' key in response for {endpoint}.")

        log.info(f"Received {len(data)} records from {endpoint}.")

        df = spark.createDataFrame(data)
        for old_col, new_col in column_renames.items():
            df = df.withColumnRenamed(old_col, new_col)

        df_tgt = df.select(*[col(new_col) for new_col in column_renames.values()])

        DuplicateValidator.validate_no_duplicates(df_tgt, key_columns=key_columns)

        log.info(f"Loading data into PostgreSQL table {target_table}...")
        load_to_postgres(df_tgt, target_table)

        log.info(f"{endpoint} ETL process completed successfully.")
        return f"{endpoint} ETL process completed successfully."

    except Exception as e:
        log.error(f"ETL failed for {endpoint}: {str(e)}")
        raise AirflowException(f"ETL failed for {endpoint}: {str(e)}")

    finally:
        spark.stop()
        log.info(f"Spark session for {endpoint} completed.")


@task
def ingest_suppliers():
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

@task
def ingest_products():
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

@task
def ingest_customers():
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
