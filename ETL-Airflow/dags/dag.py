from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import logging
from airflow.hooks.base import BaseHook

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def get_postgres_credentials():
    conn = BaseHook.get_connection('pg_conn_metamorph')
    return conn.login, conn.password, conn.host, conn.port  # Removed schema

with DAG(
    dag_id='usa_population_etl_taskflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL DAG using TaskFlow API with Spark and PostgreSQL',
    tags=['etl', 'spark'],
) as dag:

    @task()
    def load_data():
        # Get Postgres credentials inside the task
        pg_user, pg_password, pg_host, pg_port = get_postgres_credentials()

        # Hardcode database name as 'data_usa'
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/data_usa"

        # Step 1: Start Spark session
        spark = SparkSession.builder \
            .appName("DataUSA_ETL") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .config("spark.driver.extraClassPath", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("INFO")
        log.info("Spark session started.")

        # Step 2: Extract from API
        url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["data"]
            log.info(f"Retrieved {len(data_list)} records from API.")
        except Exception as e:
            log.error("Failed to fetch data from API", exc_info=True)
            raise

        # Step 3: Define schema & create DataFrame
        schema = StructType([
            StructField("id_nation", StringType(), True),
            StructField("nation", StringType(), True),
            StructField("id_year", IntegerType(), True),
            StructField("year", StringType(), True),
            StructField("population", IntegerType(), True),
            StructField("slug_nation", StringType(), True)
        ])

        rows = [
            (
                row["ID Nation"],
                row["Nation"],
                int(row["ID Year"]),
                row["Year"],
                int(row["Population"]),
                row["Slug Nation"]
            ) for row in data_list
        ]

        spark_df = spark.createDataFrame(rows, schema=schema)
        log.info("DataFrame created successfully.")
        spark_df.printSchema()
        log.info(f"Total rows in DataFrame: {spark_df.count()}")
        spark_df.show(truncate=False)

        # Step 4: Load to PostgreSQL
        properties = {
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver"
        }

        try:
            spark_df.write.jdbc(
                url=jdbc_url,
                table="population_data",
                mode="overwrite",
                properties=properties
            )
            log.info("Data written to PostgreSQL successfully!")
        except Exception as e:
            log.error("Failed to write to PostgreSQL", exc_info=True)
            raise

    # Call the task
    load_data()
