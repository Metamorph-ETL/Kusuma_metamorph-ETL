from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_task import (
    m_ingest_data_into_suppliers,
    m_ingest_data_into_products,
    m_ingest_data_into_customers,
    m_ingest_data_into_sales
    )
from tasks.supplier_performance_task import m_load_suppliers_performance
from tasks.product_performance_task import m_load_product_performance

@dag(
    dag_id="api_to_raw_ingestion_pipeline",
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 2),
    catchup=False,
    tags=["ETL"]
)
def etl_process():
   supplier_task = m_ingest_data_into_suppliers()
   product_task = m_ingest_data_into_products()
   customer_task = m_ingest_data_into_customers()
   sales_task = m_ingest_data_into_sales()
   supplier_performance_task = m_load_suppliers_performance()
   product_performance_task = m_load_product_performance()


   [supplier_task >> product_task >> customer_task >> sales_task] >> supplier_performance_task >> product_performance_task

   
dag_instance = etl_process()