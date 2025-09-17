"""
DAG para executar o script main.py do sistema de modelagens Maloka para o cliente BIBICELL
Esta DAG executa todas as modelagens disponíveis no sistema para o cliente BIBICELL.
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.operators.python_operator import PythonOperator
import os
import sys

# Adicionar o caminho do projeto ao PYTHONPATH para que os imports funcionem corretamente
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Caminho do projeto para uso nas tarefas
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar a classe ModelagemManager para uso nas DAGs
from dags.modelagens.analytics import main
from dags.modelagens.analytics.config_clientes import CLIENTES

def executar_modelagem_bibicell(**kwargs):
    """
    Função para executar modelagens específicas para o cliente BIBICELL
    """
    cliente_id = 'bibicell'
    if cliente_id not in CLIENTES:
        raise ValueError(f"Cliente {cliente_id} não encontrado na configuração")
        
    # Executa as modelagens apenas para o cliente BIBICELL
    manager = main.ModelagemManager()
    manager.atualizar_tudo(cliente_especifico=cliente_id)

@dag(
    dag_id='maloka_modelagens_bibicell',
    description='Executa todas as modelagens do sistema Maloka para o cliente BIBICELL',
    schedule='30 8 * * *',  # Executar todos os dias às 8:30h (Horário de Brasília)
    start_date=datetime(2025, 9, 10),  # Data de início
    catchup=False,
    tags=['maloka', 'modelagens', 'bibicell'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['leandro@maloka.ai'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def maloka_modelagens_bibicell():
    """DAG para executar as modelagens do sistema Maloka para o cliente BIBICELL."""
    
    # Tarefa para executar todas as modelagens para o cliente BIBICELL
    executar_modelagens = PythonOperator(
        task_id='executar_modelagens_bibicell',
        python_callable=executar_modelagem_bibicell,
        provide_context=True
    )
    
    # Configuração de dependências (ordem de execução) - opcional neste caso com apenas uma tarefa
    return executar_modelagens

# Instanciação da DAG para o cliente BIBICELL
maloka_modelagens_bibicell_dag = maloka_modelagens_bibicell()
