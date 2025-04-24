# dags/add/silver/dag_add_silver.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.layers import SilverLayer

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="add_silver",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="add_bronze",
        external_task_id="process_bronze",
        timeout=600,
        mode="poke",
    )

    def process_silver(**context):
        bronze_key = context["ti"].xcom_pull(
            dag_id="add_bronze",
            task_ids="process_bronze",
            key="bronze_key"
        )
        sl = SilverLayer()
        silver_key = sl.process(bronze_key, bk_cols=["id"])
        context["ti"].xcom_push(key="silver_key", value=silver_key)

    silver_task = PythonOperator(
        task_id="process_silver",
        python_callable=process_silver,
        provide_context=True,
    )

    wait_for_bronze >> silver_task
