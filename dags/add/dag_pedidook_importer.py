# dags/add/dag_pedidook_importer.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Garante que o diretório do main.py está no sys.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pedidook_importer'))

from pedidook_importer.main import importar_todos_endpoints

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_importar_todos_endpoints(**context):
    # Define a data de modificação como 5 dias atrás
    last_modified_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S')
    importar_todos_endpoints(last_modified_date)

with DAG(
    dag_id="pedidook_importer_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["pedidook", "importer"],
) as dag:
    importar_todos = PythonOperator(
        task_id="importar_todos_endpoints",
        python_callable=run_importar_todos_endpoints,
        provide_context=True,
    )
