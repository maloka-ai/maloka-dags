# dags/add/bronze/dag_add_bronze.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.layers import BronzeLayer

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="add_bronze",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    wait_for_transient = ExternalTaskSensor(
        task_id="wait_for_transient",
        external_dag_id="add_transient",
        external_task_id="generate_and_upload",
        timeout=600,
        mode="poke",
    )

    def process_bronze(**context):
        transient_key = context["ti"].xcom_pull(
            dag_id="add_transient",
            task_ids="generate_and_upload",
            key="transient_key"
        )
        bl = BronzeLayer()
        bronze_key = bl.process(transient_key)
        context["ti"].xcom_push(key="bronze_key", value=bronze_key)

    bronze_task = PythonOperator(
        task_id="process_bronze",
        python_callable=process_bronze,
        provide_context=True,
    )

    wait_for_transient >> bronze_task
