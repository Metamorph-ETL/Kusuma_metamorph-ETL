from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import requests
import logging
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


log = logging.getLogger("ETL_logger")
log.setLevel(logging.INFO)


#create and configure Spark session
def init_spark():
    log.info("Starting ETL process")
    spark = SparkSession.builder\
        .appName("ETL")\
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    log.info("Spark session initialized")
    return spark

class APIClient:
    BASE_URL = "http://host.docker.internal:8000"  # Replace with actual host

    def __init__(self, headers=None):
        self.session = requests.Session()
        self.session.headers.update(headers or {"Content-Type": "application/json"})

    def fetch_data(self, endpoint, auth=False):  #  Correct indentation
        url = self.BASE_URL.rstrip('/') + '/' + endpoint.lstrip('/')
        log.info(f"Making GET request to {url}")
        try:
            if 'customers' in endpoint.lower():
                self.session.headers.update({"Authorization": "Bearer YOUR_TOKEN"})
            else:
                self.session.headers.pop("Authorization", None)

            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log.error(f"API request failed: {e}")
            raise AirflowException(f"API request failed: {e}")


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

def load_to_postgres():
    conn = BaseHook.get_connection('pg_conn_metamorph')
    return conn.login, conn.password, conn.host, conn.port

def load_data_task(endpoint, target_table):
    pg_user, pg_password, pg_host, pg_port = load_to_postgres()
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/meta_morph"

    spark = init_spark()
    client = APIClient()
    data = client.fetch_data(endpoint, auth=False)

    if not data.get("data"):
        raise AirflowException("API response did not contain 'data' key or it's empty.")

    df = spark.createDataFrame(data["data"])

    # Optional: change keys per endpoint
    validation_keys = ["state", "year"] if "population" in endpoint else ["id"]
    DuplicateValidator.validate_duplicates(df, validation_keys)

    df.write.jdbc(
        url=jdbc_url,
        table=target_table,
        mode="overwrite",
        properties={
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver"
        }
    )
    log.info(f"Data successfully loaded into PostgreSQL table '{target_table}'.")
