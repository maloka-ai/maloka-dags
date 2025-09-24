from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def load_variables(**kwargs):
    """
    Carrega as variáveis de configuração do banco e as retorna via XCom
    para uso em outras DAGs/tarefas
    """
    # Importe seu módulo aqui dentro da função
    from config.airflow_variables import get_db_config_maloka
    
    # Obtém a configuração
    config = get_db_config_maloka()
    print("DB Config carregado:", {k: '***' if k == 'password' else v for k, v in config.items()})
    
    # Retorna para o XCom
    return config

# Define a DAG com execução periódica para manter as variáveis atualizadas
dag = DAG(
    'dag_load_variables',
    description='Carrega variáveis de configuração para uso em outras DAGs',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',  # Executa a cada hora para manter as variáveis atualizadas
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'max_active_runs': 1,
    }
)

# Task para carregar as variáveis
load_task = PythonOperator(
    task_id='load_variables',
    python_callable=load_variables,
    do_xcom_push=True,  # Garante que o retorno seja salvo no XCom
    dag=dag
)