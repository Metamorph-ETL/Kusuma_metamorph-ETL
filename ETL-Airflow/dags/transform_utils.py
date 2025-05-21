from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import requests
import logging
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from datetime import datetime
import os

log = logging.getLogger("ETL_logger")
log.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def init_spark():
    log.info("Creating Spark session...")
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    log.info("Spark session initialized")
    return spark

class APIClient:
    BASE_URL = "http://host.docker.internal:8000"

    def __init__(self, headers=None):
        self.session = requests.Session()
        self.session.headers.update(headers or {"Content-Type": "application/json"})

    def fetch_data(self, endpoint, auth=False):
        url = self.BASE_URL + endpoint
        log.info(f"Making GET request to {url}")
        try:
            if auth:
                token = os.getenv("API_AUTH_TOKEN", "default_token")
                self.session.headers.update({"Authorization": f"Bearer {token}"})

            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log.error(f"API request failed: {e}")
            raise AirflowException(f"API request failed: {e}")

def load_to_postgres():
    conn = BaseHook.get_connection('pg_conn_metamorph')
    return conn.login, conn.password, conn.host, conn.port

class DuplicateException(Exception):
    def __init__(self, message="Duplicate data detected during validation."):
        super().__init__(message)

class DuplicateValidator:
    @classmethod
    def validate_duplicates(cls, dataframe, key_columns):
        logging.info("Running duplicate validation on input DataFrame.")
        duplicates = dataframe.groupBy(key_columns).agg(count("*").alias("duplicate_count"))
        if duplicates.filter(duplicates["duplicate_count"] > 1).count() > 0:
            raise DuplicateException
        logging.info("No duplicates found. Validation passed.")

with DAG(
    dag_id='usa_population_etl_taskflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL DAG using TaskFlow API with Spark and PostgreSQL',
    tags=['etl', 'spark'],
) as dag:

    @task()
    def load_data_task():
        pg_user, pg_password, pg_host, pg_port = load_to_postgres()
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/data_usa"

        spark = init_spark()

        client = APIClient()
        data = client.fetch_data("/api/population", auth=False)

        if not data.get("data"):
            raise AirflowException("API response did not contain 'data' key or it's empty.")

        df = spark.createDataFrame(data["data"])

        DuplicateValidator.validate_duplicates(df, ["state", "year"])

        df.write.jdbc(
            url=jdbc_url,
            table="usa_population",
            mode="overwrite",
            properties={
                "user": pg_user,
                "password": pg_password,
                "driver": "org.postgresql.Driver"
            }
        )
        log.info("Data successfully loaded into PostgreSQL table 'usa_population'.")

    load_data_task()
