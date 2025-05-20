from pyspark.sql import SparkSession
import requests
import logging
from pyspark.sql.functions import count

log = logging.getLogger(__name__)

# Spark Initialization
def init_spark():
   # Initialize and return a Spark session.
    log.info("Creating Spark session...")
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()
    return spark

# API Client
class APIClient:
    BASE_URL = "http://host.docker.internal:8000"

    def __init__(self, headers=None):
        self.session = requests.Session()
        self.session.headers.update(headers or {"Content-Type": "application/json"})

    def fetch_data(self, endpoint, auth=False):
        #Fetch data from the API endpoint.
        url = self.BASE_URL + endpoint
        log.info(f"Making GET request to {url}")
        try:
            if auth:
                # You can modify this to use token-based auth etc.
                self.session.headers.update({"Authorization": "Bearer YOUR_TOKEN"})

            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log.error(f"API request failed: {e}")
            raise



# PostgreSQL Loader
def load_to_postgres(df, table_name, mode="append", db_config=None):

    #Load Spark DataFrame to PostgreSQL using JDBC.
    log.info(f"Loading data into PostgreSQL table {table_name}...")

    jdbc_url = "jdbc:postgresql://localhost:5432/your_db"
    jdbc_properties = {
        "user": "postgres",
        "password": "ksl2003*",
        "driver": "org.postgresql.Driver"
    }

    try:
        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=jdbc_properties)
        log.info(f"Data successfully written to {table_name}")
    except Exception as e:
        log.error(f"Failed to write data to PostgreSQL: {e}")
        raise

# Custom exception
class DuplicateException(Exception):
     def __init__(self, message="Duplicate data detected during validation."):
        # Call the base class constructor with the message
        super().__init__(message)

# Duplicate check class
class DuplicateValidator:
    """Performs validation checks to ensure uniqueness in data."""

    @classmethod
    def validate_no_duplicates(cls, dataframe, key_columns):
        logging.info("Running duplicate validation on input DataFrame.")
        duplicates = dataframe.groupBy(key_columns).agg(count("*").alias("duplicate_count"))
        if duplicates.filter(duplicates["duplicate_count"] > 1).count() > 0:
            raise DuplicateException
        logging.info("No duplicates found. Validation passed.")
