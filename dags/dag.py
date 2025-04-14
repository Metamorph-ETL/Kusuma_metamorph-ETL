from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='usa_population_etl_taskflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL DAG using TaskFlow API with Spark and PostgreSQL',
    tags=['etl', 'spark']
) as dag:

    @task()
    def run_etl():
        # Step 1: Start Spark session
        spark = SparkSession.builder \
            .appName("DataUSA_ETL") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .config("spark.driver.extraClassPath", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("INFO")

        print("Spark session started.")

        # Step 2: Extract from API
        url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["data"]
            print(f"Retrieved {len(data_list)} records from API.")
        except Exception as e:
            print("Failed to fetch data from API:", e)
            return

        # Step 3: Define schema & convert to Spark DataFrame
        schema = StructType([
            StructField("id_nation", StringType(), True),
            StructField("nation", StringType(), True),
            StructField("id_year", IntegerType(), True),
            StructField("year", StringType(), True),
            StructField("population", IntegerType(), True),
            StructField("slug_nation", StringType(), True)
        ])

        # Convert raw API data to list of tuples matching the schema
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

        print(" DataFrame schema:")
        spark_df.printSchema()
        print(f"Total rows in DataFrame: {spark_df.count()}")
        spark_df.show(truncate=False)

        # Step 4: Load to PostgreSQL
        jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
        properties = {
            "user": "postgres",
            "password": "ksl2003*",
            "driver": "org.postgresql.Driver"
        }

        try:
            spark_df.write.jdbc(
                url=jdbc_url,
                table="population_data",
                mode="overwrite",
                properties=properties
            )
            print(" Data written to PostgreSQL successfully!")
        except Exception as e:
            print(" Failed to write to PostgreSQL:", e)

    run_etl()
