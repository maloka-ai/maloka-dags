from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def teste():
    # teste
    print("Executou DAG de teste...")

with DAG(
    dag_id="dagTeste02",
    start_date=datetime(2025, 8, 2),
    schedule_interval="*/15 * * * *",   # a cada 15 minutos
    catchup=False,
    tags=["teste_dag_modelgaens"],
) as dag:

    teste_task = PythonOperator(
        task_id="teste_task",
        python_callable=teste,
    )
