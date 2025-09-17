from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test3():
    # teste
    print("Executou DAG de teste 333 ...")

with DAG(
    dag_id="dagTeste03",
    start_date=datetime(2025, 8, 2),
    schedule_interval="*/15 * * * *",   # a cada 15 minutos
    catchup=False,
    tags=["teste_dag_modelgaens"],
) as dag:

    teste_task = PythonOperator(
        task_id="teste_3_task",
        python_callable=test3,
    )
