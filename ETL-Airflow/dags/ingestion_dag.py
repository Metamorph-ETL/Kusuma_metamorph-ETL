from airflow.decorators import dag, task
from datetime import datetime
from tasks.ingestion_task import ingest_suppliers, ingest_customers, ingest_products

@dag(
    dag_id="api_to_raw_ingestion_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 2),
    catchup=False,
    tags=["etl", "api", "pyspark", "postgres"],
)
def etl_pipeline():
    ingest_suppliers()
    ingest_customers()
    ingest_products()

dag_instance = etl_pipeline()
