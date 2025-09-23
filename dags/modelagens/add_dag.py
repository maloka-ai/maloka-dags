"""
DAG para executar o script main.py do sistema de modelagens Maloka para o cliente ADD
Esta DAG executa todas as modelagens disponíveis no sistema para o cliente ADD.
Esta DAG inclui um mecanismo de validação para verificar se o banco de dados está pronto para atualização.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

# Adicionar o caminho do projeto ao PYTHONPATH para que os imports funcionem corretamente
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Caminho do projeto para uso nas tarefas
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar a classe ModelagemManager para uso nas DAGs
from dags.modelagens.analytics import main
from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import DB_CONFIG_MALOKA

# Importar funções de validação de atualização do banco de dados
from utils.triggers import (
    criar_deferrable_task_atualizacao_banco, 
    registrar_sucesso_atualizacao,
    registrar_falha_atualizacao
)

def executar_modelagem_add(**kwargs):
    """
    Função para executar modelagens específicas para o cliente ADD
    """
    cliente_id = 'add'
    if cliente_id not in CLIENTES:
        raise ValueError(f"Cliente {cliente_id} não encontrado na configuração")
        
    # Usamos DB_CONFIG_MALOKA para ter acesso direto às configurações de conexão do banco
    # Isso pode ser útil para logs ou para passar para funções que precisam de conexão direta
    print(f"Configurações de banco para cliente {cliente_id}:")
    print(f"Host: {DB_CONFIG_MALOKA['host']}")
    print(f"Port: {DB_CONFIG_MALOKA['port']}")
    print(f"User: {DB_CONFIG_MALOKA['user']}")
    
    # Executa as modelagens apenas para o cliente ADD
    manager = main.ModelagemManager()
    manager.atualizar_tudo(cliente_especifico=cliente_id)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['leandro@maloka.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configurações para conexão com o banco e cliente
CLIENTE_ID = 'add'
INTERVALO_VERIFICACAO_MIN = 15  # Verificar a cada 15 minutos

# Agora estamos usando diretamente a configuração do banco através de DB_CONFIG_MALOKA
print(f"Usando configuração de banco: Host={DB_CONFIG_MALOKA['host']}, Port={DB_CONFIG_MALOKA['port']}")

# Definição da DAG usando with
with DAG(
    dag_id='dag_modelagem_add',
    description='Executa todas as modelagens do sistema Maloka para o cliente ADD',
    schedule_interval='30 8 * * *',  # Executar todos os dias às 8:30h (Horário de Brasília)
    start_date=datetime(2025, 9, 10),  # Data de início
    catchup=False,
    tags=['maloka', 'modelagens', 'add'],
    default_args=default_args
) as dag:
    
    # Task para verificar se o banco de dados está pronto para atualização
    verificar_atualizacao = criar_deferrable_task_atualizacao_banco(
        task_id='verificar_atualizacao_banco',
        cliente_id=CLIENTE_ID,
        intervalo_verificacao_minutos=INTERVALO_VERIFICACAO_MIN,
        dag=dag,
        params={
            'success_task_id': 'executar_modelagens_add',
            'wait_task_id': 'aguardar_banco_atualizado'
        }
    )()
    
    # Task de espera quando o banco não está pronto para atualização
    aguardar_banco = EmptyOperator(
        task_id='aguardar_banco_atualizado',
        trigger_rule='all_done'
    )
    
    # Tarefa para executar todas as modelagens para o cliente ADD
    executar_modelagens = PythonOperator(
        task_id='executar_modelagens_add',
        python_callable=executar_modelagem_add,
        provide_context=True
    )
    
    # Task para registrar o sucesso da atualização
    registrar_sucesso = PythonOperator(
        task_id='registrar_sucesso',
        python_callable=registrar_sucesso_atualizacao,
        op_kwargs={
            'cliente_id': CLIENTE_ID
        },
        trigger_rule='all_success'
    )
    
    # Task para registrar falha na atualização
    registrar_falha = PythonOperator(
        task_id='registrar_falha',
        python_callable=registrar_falha_atualizacao,
        op_kwargs={
            'cliente_id': CLIENTE_ID
        },
        trigger_rule='one_failed',
        on_failure_callback=None  # Não propagar falha para evitar recursão
    )
    
    # Definir a sequência das tarefas
    verificar_atualizacao >> executar_modelagens
    verificar_atualizacao >> aguardar_banco
    executar_modelagens >> registrar_sucesso
    executar_modelagens >> registrar_falha
    
    # Aguardar_banco não tem próxima tarefa - a DAG termina e será tentada novamente no próximo agendamento
