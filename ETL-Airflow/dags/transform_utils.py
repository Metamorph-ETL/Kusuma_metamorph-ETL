from pyspark.sql import SparkSession
import requests
from secret_key import PG_PWD, USERNAME,PASSWORD
import logging
from airflow.exceptions import AirflowException
from pyspark.sql.functions import col, count

# Initialize logger
log = logging.getLogger("etl_logger")
log.setLevel(logging.INFO)

def create_session():
    log.info("Initialising the spark Session")
    
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()


    spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.enable", "true"
    )
    spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.json.keyfile",
    "/usr/local/airflow/jars/meta-morph-d-eng-pro-view-key.json"
    )
       
    
    log.info("spark session created")
    return spark

#Extractor class handles API data extraction 
class Extractor:
    def __init__(self, endpoint, token=None):
        self.base_url = "http://host.docker.internal:8000" 
        self.url = f"{self.base_url}{endpoint}"
        self.token = token

# Automatically fetch token if it's a customer API and no token is provided
        if "customers" in endpoint and token is None:  
            self.token = self._get_token()

# Private method to fetch bearer token from TOKEN_URL
    def _get_token(self):
        token_url = f"{self.base_url}/token"
        try:
            response = requests.post(token_url, data={
                "username": USERNAME,
                "password": PASSWORD,  
            })

            if response.status_code == 200:
                token = response.json().get("access_token")
                return token
            else:
                raise AirflowException(f"Failed to fetch token. Status: {response.status_code}")
        except Exception as e:
            log.error(f"Token fetch failed: {str(e)}", exc_info=True)
      
    def extract_data(self):
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
  
        response = requests.get(self.url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])
            log.info(f"Extracted {len(data)} records from {self.url}")
            return data
        else:
            error_msg = f"Failed to fetch from {self.url}. Status: {response.status_code}"
            log.error(error_msg)
            raise AirflowException(error_msg)

# Custom exception for duplicate detection

class DuplicateException(Exception):
    def __init__(self, message):
        super().__init__(message)

class Duplicate_check:
    @classmethod
    def has_duplicates(cls, df, primary_key_list):
        log.info("Checking for duplicates in the given data")
        grouped_df = df.groupBy(primary_key_list)\
                      .agg(count('*').alias('cnt'))\
                      .filter(col("cnt") > 1)
        if grouped_df.count() > 0:
            raise DuplicateException(f"Found duplicates in columns: {primary_key_list}")
        log.info("No duplicates found")

    
def load_to_postgres(data_frame, table_name, mode="overwrite"):
    log.info(f"Loading data into PostgreSQL table: {table_name}") 
    df = data_frame.write.format("jdbc")\
            .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{table_name}") \
            .option("user", "postgres") \
            .option("password", PG_PWD) \
            .mode(mode) \
                .save()
    
    log.info("Loaded data successfully")
    return f"Task for loading data into {table_name} completed successfully"


def end_session(spark):
        log.info("Stopping Spark session...")
        spark.stop()
        log.info("Spark session stopped.")

def read_from_postgres(spark, table_name):
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/meta_morph"
    properties = {
        "user": "postgres",
        "password": PG_PWD,
        "driver": "org.postgresql.Driver"
    }
    log.info(f"Reading table {table_name} from PostgreSQL")
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=properties
    )
    log.info(f"Read {df.count()} records from {table_name}")
    return df        