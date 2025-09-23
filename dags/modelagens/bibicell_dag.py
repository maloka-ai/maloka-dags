"""
DAG para # Adicionar o caminho do projeto ao PYTHONPATH para que os imports funcionem corretamente
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importar nosso módulo de logging personalizado
from utils.airflow_logging import configurar_logger, log_task_info

# Configuração do logger para ser usado nas tarefas do Airflow
logger = configurar_logger(__name__)

# Caminho do projeto para uso nas tarefas
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file____))))tar o script main.py do sistema de modelagens Maloka para o cliente BIBICELL
Esta DAG executa todas as modelagens disponíveis no sistema para o cliente BIBICELL.
Esta DAG inclui um mecanismo de validação para verificar se o banco de dados está pronto para atualização.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys
import logging

# Adicionar o caminho do projeto ao PYTHONPATH para que os imports funcionem corretamente
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importar nosso módulo de logging personalizado
from utils.airflow_logging import configurar_logger, log_task_info
# Configuração do logger para ser usado nas tarefas do Airflow
logger = configurar_logger(__name__)

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
from utils.airflow_callbacks import falha_task_callback, sucesso_task_callback


def executar_modelagem_bibicell(**kwargs):
    """
    Função para executar modelagens específicas para o cliente BIBICELL
    """
    cliente_id = 'bibicell'
    log_task_info(kwargs, f"Iniciando execução das modelagens para cliente {cliente_id}")
    
    if cliente_id not in CLIENTES:
        logger.error(f"Cliente {cliente_id} não encontrado na configuração")
        raise ValueError(f"Cliente {cliente_id} não encontrado na configuração")
    
    # Usamos DB_CONFIG_MALOKA para ter acesso direto às configurações de conexão do banco
    # Isso pode ser útil para logs ou para passar para funções que precisam de conexão direta
    log_task_info(kwargs, f"Configurações de banco para cliente {cliente_id}:")
    log_task_info(kwargs, f"Host: {DB_CONFIG_MALOKA['host']}")
    log_task_info(kwargs, f"Port: {DB_CONFIG_MALOKA['port']}")
    log_task_info(kwargs, f"User: {DB_CONFIG_MALOKA['user']}")
    
    try:
        # Executa as modelagens apenas para o cliente BIBICELL
        log_task_info(kwargs, f"Executando modelagens para cliente {cliente_id}")
        # Passa o contexto do Airflow para o ModelagemManager para que os logs sejam visíveis no Airflow
        manager = main.ModelagemManager(airflow_context=kwargs)
        manager.atualizar_tudo(cliente_especifico=cliente_id)
        log_task_info(kwargs, f"Modelagens para cliente {cliente_id} executadas com sucesso")
    except Exception as e:
        logger.error(f"Erro ao executar modelagens para cliente {cliente_id}: {str(e)}")
        # Re-lança a exceção para o Airflow detectar a falha
        raise

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
CLIENTE_ID = 'bibicell'
INTERVALO_VERIFICACAO_MIN = 15  # Verificar a cada 15 minutos

# Agora estamos usando diretamente a configuração do banco através de DB_CONFIG_MALOKA
logger.info(f"Usando configuração de banco: Host={DB_CONFIG_MALOKA['host']}, Port={DB_CONFIG_MALOKA['port']}")

# Definição da DAG usando with
with DAG(
    dag_id='dag_modelagem_bibicell',
    description='Executa todas as modelagens do sistema Maloka para o cliente BIBICELL',
    schedule_interval='30 8 * * *',  # Executar todos os dias às 8:30h (Horário de Brasília)
    start_date=datetime(2025, 9, 10),  # Data de início
    catchup=False,
    tags=['maloka', 'modelagens', 'bibicell'],
    default_args=default_args
) as dag:
    
    # Task para verificar se o banco de dados está pronto para atualização
    verificar_atualizacao = criar_deferrable_task_atualizacao_banco(
        task_id='verificar_atualizacao_banco',
        cliente_id=CLIENTE_ID,
        intervalo_verificacao_minutos=INTERVALO_VERIFICACAO_MIN,
        dag=dag,
        params={
            'success_task_id': 'executar_modelagens_bibicell',
            'wait_task_id': 'aguardar_banco_atualizado'
        }
    )
    
    # Task de espera quando o banco não está pronto para atualização
    aguardar_banco = EmptyOperator(
        task_id='aguardar_banco_atualizado',
        trigger_rule='all_done'
    )
    
    # Tarefa para executar todas as modelagens para o cliente BIBICELL
    executar_modelagens = PythonOperator(
        task_id='executar_modelagens_bibicell',
        python_callable=executar_modelagem_bibicell,
        provide_context=True,
        on_failure_callback=falha_task_callback,
        on_success_callback=sucesso_task_callback
    )

    # Função para logging detalhado no sucesso
    def registrar_sucesso_com_logs(**kwargs):
        log_task_info(kwargs, f"Registrando sucesso para cliente {CLIENTE_ID}")
        resultado = registrar_sucesso_atualizacao(cliente_id=CLIENTE_ID)
        log_task_info(kwargs, f"Resultado do registro de sucesso: {resultado}")
        return resultado

    # Task para registrar o sucesso da atualização
    registrar_sucesso = PythonOperator(
        task_id='registrar_sucesso',
        python_callable=registrar_sucesso_com_logs,
        provide_context=True,
        trigger_rule='all_success'
    )
    
    # Função para logging detalhado na falha
    def registrar_falha_com_logs(**kwargs):
        log_task_info(kwargs, f"Registrando falha para cliente {CLIENTE_ID}")
        try:
            resultado = registrar_falha_atualizacao(cliente_id=CLIENTE_ID)
            log_task_info(kwargs, f"Resultado do registro de falha: {resultado}")
            return resultado
        except Exception as e:
            logger.error(f"Erro ao registrar falha: {str(e)}")
            # Não relançamos a exceção para evitar recursão

    # Task para registrar falha na atualização
    registrar_falha = PythonOperator(
        task_id='registrar_falha',
        python_callable=registrar_falha_com_logs,
        provide_context=True,
        trigger_rule='one_failed',
        on_failure_callback=None  # Não propagar falha para evitar recursão
    )
    
    # Definir a sequência das tarefas
    verificar_atualizacao >> executar_modelagens
    verificar_atualizacao >> aguardar_banco
    executar_modelagens >> registrar_sucesso
    executar_modelagens >> registrar_falha
    
    # Aguardar_banco não tem próxima tarefa - a DAG termina e será tentada novamente no próximo agendamento
