from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def teste_rapido():
    # Importe seu módulo aqui dentro da função
    from config.airflow_variables import get_db_config_maloka
    
    config = get_db_config_maloka()
    print("Config:", config)

dag = DAG(
    'teste_rapido',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
)

PythonOperator(
    task_id='teste',
    python_callable=teste_rapido,
    dag=dag
)