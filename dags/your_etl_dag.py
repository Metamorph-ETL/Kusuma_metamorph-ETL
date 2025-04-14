from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Airflow!")

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello
    )
