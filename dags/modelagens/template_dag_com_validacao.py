"""
Template de DAG com verificação de banco de dados atualizado

Este template demonstra como implementar uma DAG que verifica se o banco de dados possui
dados importados que ainda não foram processados pelas modelagens.

A verificação busca na tabela configuracao.log_processamento_dados o registro mais recente
para o cliente e valida se a coluna data_execucao_modelagem é None. Se for, significa que
existem dados importados aguardando processamento.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import os
import sys

# Adicionar o caminho do projeto ao PYTHONPATH para que os imports funcionem corretamente
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importar funções de validação de atualização do banco de dados
from utils.triggers import (
    criar_deferrable_task_atualizacao_banco, 
    registrar_sucesso_atualizacao,
    registrar_falha_atualizacao
)

# Configurações da DAG
CLIENTE_ID = 'NOME_DO_CLIENTE'  # Substitua pelo ID do cliente
DB_CONN_ID = 'CONEXAO_BANCO'    # Substitua pelo ID da conexão do banco no Airflow
INTERVALO_VERIFICACAO_MIN = 15  # Verificar a cada 15 minutos

# Função para executar a tarefa principal
def executar_tarefa_principal(**kwargs):
    """
    Função que executa a tarefa principal da DAG
    Substitua esta função pela lógica específica da sua DAG
    """
    print(f"Executando tarefa principal para cliente {CLIENTE_ID}")
    # Aqui vai o código específico da sua DAG
    return True

# Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['seu.email@empresa.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    dag_id=f'dag_{CLIENTE_ID}_com_validacao',  # Ajuste o ID conforme necessário
    description=f'Executa processamento para {CLIENTE_ID} com validação de banco de dados',
    schedule_interval='0 9 * * *',  # Executar todos os dias às 9h
    start_date=datetime(2025, 9, 1),  # Ajuste a data conforme necessário
    catchup=False,
    tags=['validacao_banco', CLIENTE_ID],
    default_args=default_args
) as dag:
    
    # Task para verificar se o banco de dados está pronto para atualização
    verificar_atualizacao = criar_deferrable_task_atualizacao_banco(
        task_id='verificar_atualizacao_banco',
        conn_id=DB_CONN_ID,
        cliente_id=CLIENTE_ID,
        intervalo_verificacao_minutos=INTERVALO_VERIFICACAO_MIN,
        params={
            'success_task_id': 'executar_tarefa_principal',
            'wait_task_id': 'aguardar_banco_atualizado'
        }
    )
    
    # Task de espera quando o banco não está pronto para atualização
    aguardar_banco = EmptyOperator(
        task_id='aguardar_banco_atualizado',
        trigger_rule='all_done'
    )
    
    # Tarefa principal que será executada após a validação
    tarefa_principal = PythonOperator(
        task_id='executar_tarefa_principal',
        python_callable=executar_tarefa_principal,
        provide_context=True
    )
    
    # Task para registrar o sucesso da atualização
    registrar_sucesso = PythonOperator(
        task_id='registrar_sucesso',
        python_callable=registrar_sucesso_atualizacao,
        op_kwargs={
            'conn_id': DB_CONN_ID,
            'cliente_id': CLIENTE_ID
        },
        trigger_rule='all_success'
    )
    
    # Task para registrar falha na atualização
    registrar_falha = PythonOperator(
        task_id='registrar_falha',
        python_callable=registrar_falha_atualizacao,
        op_kwargs={
            'conn_id': DB_CONN_ID,
            'cliente_id': CLIENTE_ID
        },
        trigger_rule='one_failed',
        on_failure_callback=None  # Não propagar falha para evitar recursão
    )
    
    # Definir a sequência das tarefas
    verificar_atualizacao >> [tarefa_principal, aguardar_banco]
    tarefa_principal >> registrar_sucesso
    tarefa_principal >> registrar_falha
    
    # Aguardar_banco não tem próxima tarefa - a DAG termina e será tentada novamente no próximo agendamento
