from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def normalizar_texto_spede():
    # Lógica para normalizar texto do Spede
    print("Normalizando texto do Spede...")

with DAG(
    dag_id="dagNormalizarTextoSpede",
    start_date=datetime(2025, 8, 2),
    schedule_interval="*/15 * * * *",   # a cada 15 minutos
    catchup=False,
    tags=["normalizacao", "texto", "spede"],
) as dag:

    limpar_task = PythonOperator(
        task_id="normalizar_texto",
        python_callable=normalizar_texto_spede,
    )
