# dags/add/dag_add_gold_postgres.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# Conexões e bucket
AWS_CONN_ID     = "s3-conn-add"
POSTGRES_CONN   = "ADD-POSTGRES-POCDASHBOARD"
BUCKET          = "malokaai"
PREFIX          = "add/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="add_gold_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["add", "gold", "postgresapplication"],
) as dag:

    def silver_to_gold(**context):
        # Monta chaves de S3 para silver e gold
        ds_nodash   = context["ds"].replace("-", "")
        silver_key  = f"{PREFIX}silver/dados_20250426.parquet"
        gold_key    = f"{PREFIX}gold/dados_{ds_nodash}.parquet"

        # Lê Parquet da camada silver
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
        obj = s3.get_object(Bucket=BUCKET, Key=silver_key)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))

        # Aqui você pode aplicar transformação extra na GOLD, se quiser
        # Exemplo: df["valor_ajustado"] = df["valor"] * 1.1

        # Grava o Parquet na camada gold
        out_buf = BytesIO()
        df.to_parquet(out_buf, index=False)
        out_buf.seek(0)
        s3.put_object(Bucket=BUCKET, Key=gold_key, Body=out_buf.getvalue())

        # Salva a key para a próxima tarefa
        context["ti"].xcom_push(key="gold_key", value=gold_key)

    task_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
        provide_context=True,
    )

    def gold_to_postgres(**context):
        # Recupera a key do gold
        gold_key = context["ti"].xcom_pull(key="gold_key")
        if not gold_key:
            raise ValueError("gold_key não encontrado no XCom")

        # Lê Parquet da camada gold
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
        obj = s3.get_object(Bucket=BUCKET, Key=gold_key)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))

        # Prepara dados para upsert
        rows      = [tuple(x) for x in df.values]
        columns   = list(df.columns)  # ['id','valor','data_extracao','bk_columns']
        table     = "dados_gold"
        pk_cols   = ["id", "bk_columns"]
        non_pk    = [c for c in columns if c not in pk_cols]
        update_sql = ", ".join(f"{c}=EXCLUDED.{c}" for c in non_pk)

        cols_sql    = ", ".join(columns)
        placeholder = "(" + ",".join(["%s"] * len(columns)) + ")"
        upsert_sql  = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES %s
            ON CONFLICT ({', '.join(pk_cols)})
            DO UPDATE SET {update_sql};
        """

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        conn    = pg_hook.get_conn()
        cursor  = conn.cursor()
        execute_values(cursor, upsert_sql, rows, template=placeholder)
        conn.commit()
        cursor.close()

    task_gold_to_postgres = PythonOperator(
        task_id="gold_to_postgres",
        python_callable=gold_to_postgres,
        provide_context=True,
    )

    task_silver_to_gold >> task_gold_to_postgres
