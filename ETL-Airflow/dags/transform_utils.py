import logging
import os
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

log = logging.getLogger(__name__)

# --- Spark Initialization ---
def init_spark(app_name="IngestionTask", jar_path="/opt/spark/jars/postgresql-42.7.3.jar"):
    log.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jar_path) \
        .getOrCreate()
    log.info("Spark session initialized.")
    return spark

# --- API Client ---
class APIClient:
    def __init__(self, base_url="http://host.docker.internal:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.token = Variable.get("api_token", default_var="")

    def fetch_data(self, endpoint, auth=False):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        log.info(f"Making GET request to {url}")

        try:
            if auth or "customers" in endpoint.lower():
                self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            else:
                self.session.headers.pop("Authorization", None)

            response = self.session.get(url)
            response.raise_for_status()
            log.info(f"Data fetched successfully from {endpoint}")
            return response.json()

        except requests.RequestException as e:
            log.error(f"API request failed: {e}")
            raise AirflowException(f"API request failed: {e}")

# --- Custom Exceptions ---
class DuplicateException(Exception):
    def __init__(self, message="Duplicate data detected during validation."):
        super().__init__(message)

# --- Duplicate Validator ---
class DuplicateValidator:
    @classmethod
    def validate_duplicates(cls, dataframe, key_columns):
        log.info("Running duplicate validation on input DataFrame...")
        duplicates = dataframe.groupBy(key_columns).agg(count("*").alias("duplicate_count"))
        count_duplicates = duplicates.filter(duplicates["duplicate_count"] > 1).count()

        if count_duplicates > 0:
            log.error(f"Duplicate validation failed. {count_duplicates} duplicates found.")
            raise DuplicateException(f"{count_duplicates} duplicates found.")

        log.info("No duplicates found. Validation passed.")

# --- PostgreSQL Loader ---
def load_postgres_credentials(conn_id='pg_conn_metamorph'):
    conn = BaseHook.get_connection(conn_id)
    return conn.login, conn.password, conn.host, conn.port, conn.schema or "meta_morph"

def load_data_task_from_df(df, target_table, write_mode="overwrite"):
    log.info(f"Starting write to PostgreSQL table: {target_table}")
    pg_user, pg_password, pg_host, pg_port, pg_db = load_postgres_credentials()
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    try:
        df.write.jdbc(
            url=jdbc_url,
            table=target_table,
            mode=write_mode,
            properties={
                "user": pg_user,
                "password": pg_password,
                "driver": "org.postgresql.Driver"
            }
        )
        log.info(f"Data successfully loaded into PostgreSQL table '{target_table}'.")
    except Exception as e:
        log.error(f"Failed to write to PostgreSQL: {e}")
        raise AirflowException(f"PostgreSQL load failed: {e}")
