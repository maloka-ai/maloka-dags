from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def minha_mensagem():
    print("Olá! Esta é a minha DAG básica funcionando no Airflow.")

with DAG(
    dag_id="exemplo_print_mensagem",
    start_date=datetime(2025, 9, 9),
    schedule=None,   # <-- Trocar aqui!
    catchup=False
) as dag:

    tarefa_print = PythonOperator(
        task_id="print_mensagem",
        python_callable=minha_mensagem
    )
