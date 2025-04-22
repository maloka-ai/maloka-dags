from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import csv
import tempfile
import os

def generate_and_upload(**kwargs):
    # 1) Dados de exemplo
    data = [
        {"id": i, "nome": f"nome_{i}", "valor": f"valor_{i}"}
        for i in range(1, 6)
    ]

    # 2) Cria arquivo temporário
    tmp_dir = tempfile.gettempdir()
    filename = os.path.join(tmp_dir, "dados_base_test.csv")

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter="|")
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # 3) Faz upload para o S3
    hook = S3Hook(aws_conn_id="s3_malokaai")
    hook.load_file(
        filename=filename,
        key="add/transient/dados_base_test.csv",
        bucket_name="malokaai",
        replace=True
    )
    # opcional: deletar arquivo local
    os.remove(filename)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="example_dados_base_test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example", "s3"],
) as dag:

    task_generate_upload = PythonOperator(
        task_id="generate_and_upload_csv",
        python_callable=generate_and_upload,
        provide_context=True,
    )
