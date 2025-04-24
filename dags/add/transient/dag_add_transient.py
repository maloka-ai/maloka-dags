# dags/add/transient/dag_add_transient.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from utils.layers import TransientLayer

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="add_transient",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def generate_and_upload(**context):
        # exemplo de dataframe; troque pela sua lógica real
        data = {"id": [1, 2, 3], "valor": [100, 200, 300]}
        df   = pd.DataFrame(data)
        fname = f"dados_{datetime.utcnow():%Y%m%d}.csv"

        tl = TransientLayer()
        transient_key = tl.write(df, fname)

        # disponibiliza para as próximas camadas
        context["ti"].xcom_push(key="transient_key", value=transient_key)

    generate = PythonOperator(
        task_id="generate_and_upload",
        python_callable=generate_and_upload,
        provide_context=True,
    )
