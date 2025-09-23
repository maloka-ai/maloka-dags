"""
Módulo de utilitários para callbacks no Airflow
"""
import logging
from typing import Dict, Any
from airflow.models import TaskInstance, DagRun
from utils.airflow_logging import log_task_info

# Configure o logger para este módulo
logger = logging.getLogger(__name__)

def falha_task_callback(context: Dict[str, Any]) -> None:
    """
    Callback para ser usado quando uma task falha.
    Registra detalhes da falha nos logs e pode enviar notificações adicionais.
    
    Args:
        context: O contexto da tarefa fornecido pelo Airflow
    """
    task_instance = context.get('task_instance')
    if not task_instance:
        logger.error("Contexto inválido no callback de falha: task_instance não encontrada")
        return
    
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    exception = context.get('exception')
    
    # Registrar informações básicas sobre a falha
    log_task_info(context, f"Task falhou: {task_id}", nivel="error")
    
    # Registrar a exceção, se disponível
    if exception:
        log_task_info(context, f"Exceção: {str(exception)}", nivel="error")
    
    # Registrar informações adicionais úteis para depuração
    log_task_info(context, f"Data de execução: {task_instance.execution_date}", nivel="error")
    
    # Registrar a duração da tarefa, se disponível
    try:
        duracao = task_instance.duration
        if duracao:
            log_task_info(context, f"Duração da tarefa: {duracao} segundos", nivel="error")
    except (AttributeError, TypeError):
        # A duração pode não estar disponível
        pass
    
    # Aqui você poderia implementar lógica adicional:
    # - Enviar alertas para sistemas externos
    # - Registrar em uma tabela de falhas no banco de dados
    # - Tentar operações de recuperação


def sucesso_task_callback(context: Dict[str, Any]) -> None:
    """
    Callback para ser usado quando uma task é concluída com sucesso.
    Registra detalhes da execução bem-sucedida nos logs.
    
    Args:
        context: O contexto da tarefa fornecido pelo Airflow
    """
    task_instance = context.get('task_instance')
    if not task_instance:
        logger.error("Contexto inválido no callback de sucesso: task_instance não encontrada")
        return
    
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    
    # Registrar informações básicas sobre o sucesso
    log_task_info(context, f"Task concluída com sucesso: {task_id}", nivel="info")
    
    # Registrar a duração da tarefa, se disponível
    try:
        duracao = task_instance.duration
        if duracao:
            log_task_info(context, f"Duração da tarefa: {duracao} segundos", nivel="info")
    except (AttributeError, TypeError):
        # A duração pode não estar disponível
        pass
    
    # Registrar parâmetros ou retornos específicos, se necessário
    try:
        xcom_valor = task_instance.xcom_pull(task_ids=task_id)
        if xcom_valor is not None:
            log_task_info(context, f"Valor retornado pela task: {str(xcom_valor)}", nivel="info")
    except Exception as e:
        # Não é crítico se não conseguirmos obter o xcom
        logger.debug(f"Não foi possível obter XCom para a task {task_id}: {str(e)}")
