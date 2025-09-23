"""
Módulo de utilitários para logs no Airflow
"""
import logging
from airflow.models import TaskInstance
from typing import Dict, Any, Optional
from datetime import datetime

def configurar_logger(nome_modulo: str) -> logging.Logger:
    """
    Configura um logger para uso nas DAGs do Airflow.
    
    Args:
        nome_modulo: Nome do módulo para o logger
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(nome_modulo)
    
    # O Airflow já configura os handlers, então não precisamos fazer isso
    # Esta função é principalmente para centralizar a criação do logger
    
    return logger

def formatar_log_airflow(
    dag_id: Optional[str] = None, 
    task_id: Optional[str] = None, 
    execution_date: Optional[datetime] = None,
    mensagem: str = ""
) -> str:
    """
    Formata uma mensagem de log com informações do Airflow.
    
    Args:
        dag_id: ID da DAG
        task_id: ID da tarefa
        execution_date: Data de execução
        mensagem: Mensagem a ser logada
    
    Returns:
        Mensagem formatada
    """
    prefixo_dag = f"[{dag_id}]" if dag_id else ""
    prefixo_task = f"[{task_id}]" if task_id else ""
    prefixo_data = f"[{execution_date.isoformat()}]" if execution_date else ""
    
    prefixo = " ".join(filter(None, [prefixo_dag, prefixo_task, prefixo_data]))
    
    if prefixo:
        return f"{prefixo} {mensagem}"
    return mensagem

def log_task_info(context: Dict[str, Any], mensagem: str, nivel: str = 'info') -> None:
    """
    Registra uma mensagem de log com informações da task.
    
    Args:
        context: O contexto fornecido pelo Airflow
        mensagem: A mensagem a ser logada
        nivel: O nível de log ('info', 'warning', 'error', 'debug')
    """
    logger = logging.getLogger('airflow.task')
    
    # Extrair informações do contexto
    task_instance = context.get('task_instance')
    
    if task_instance and isinstance(task_instance, TaskInstance):
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        try:
            execution_date = task_instance.execution_date
        except AttributeError:
            # A partir do Airflow 2.2, execution_date foi substituído por run_id
            # Neste caso, apenas não incluímos a data
            execution_date = None
        
        mensagem_formatada = formatar_log_airflow(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            mensagem=mensagem
        )
    else:
        mensagem_formatada = mensagem
    
    # Registrar a mensagem no nível apropriado
    if nivel == 'info':
        logger.info(mensagem_formatada)
    elif nivel == 'warning':
        logger.warning(mensagem_formatada)
    elif nivel == 'error':
        logger.error(mensagem_formatada)
    elif nivel == 'debug':
        logger.debug(mensagem_formatada)
    else:
        # Padrão é info
        logger.info(mensagem_formatada)
