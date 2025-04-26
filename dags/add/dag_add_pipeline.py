from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd

from utils.layers import TransientLayer, BronzeLayer, SilverLayer
from config.settings import BUCKET_NAME

# Parâmetros específicos deste pipeline
AWS_CONN_ID    = "s3-conn-add"
COMPANY_PREFIX = "add/"  # prefixo no bucket

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="add_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["add", "multi-layer"]
) as dag:

    with TaskGroup("transient", tooltip="Raw ingestion") as transient_tg:
        def gen_and_upload(**context):
            data = {"id": [1, 2, 3], "valor": [100, 200, 300]}
            df   = pd.DataFrame(data)
            fname = f"dados_{datetime.utcnow():%Y%m%d}.csv"

            tl = TransientLayer(prefix=COMPANY_PREFIX, aws_conn_id=AWS_CONN_ID)
            transient_key = tl.write(df, fname)
            context["ti"].xcom_push(key="transient_key", value=transient_key)

        generate = PythonOperator(
            task_id="generate_and_upload",
            python_callable=gen_and_upload,
            provide_context=True,
        )

    with TaskGroup("bronze", tooltip="Bronze transformation") as bronze_tg:
        def bronze_process(**context):
            key_in = context["ti"].xcom_pull(
                task_ids="transient.generate_and_upload",
                key="transient_key"
            )
            bl = BronzeLayer(prefix=COMPANY_PREFIX, aws_conn_id=AWS_CONN_ID)
            bronze_key = bl.process(key_in)
            context["ti"].xcom_push(key="bronze_key", value=bronze_key)

        process_bronze = PythonOperator(
            task_id="process",
            python_callable=bronze_process,
            provide_context=True,
        )

    with TaskGroup("silver", tooltip="Silver (upsert + hygiene)") as silver_tg:
        def silver_process(**context):
            bronze_key = context["ti"].xcom_pull(
                task_ids="bronze.process",
                key="bronze_key"
            )
            sl = SilverLayer(prefix=COMPANY_PREFIX, aws_conn_id=AWS_CONN_ID)
            sl.process(bronze_key, bk_cols=["id"])

        process_silver = PythonOperator(
            task_id="process",
            python_callable=silver_process,
            provide_context=True,
        )

    # Orquestração: Transient → Bronze → Silver
    transient_tg >> bronze_tg >> silver_tg