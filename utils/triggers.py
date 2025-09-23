"""
Módulo para gerenciar triggers de DAGs baseados em condições
"""
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import State
from airflow.exceptions import AirflowException

import asyncio
import os
import sys
from datetime import timedelta
from typing import Dict, Any, Optional

# Adicionar caminho para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.airflow_variables import DB_CONFIG_MALOKA
from .database import verificar_atualizacao_permitida, registrar_tentativa_atualizacao


class BancoDadosAtualizadoTrigger(BaseTrigger):
    """
    Trigger que verifica se o banco de dados está pronto para atualização.
    
    Este trigger consulta a tabela log_processamento_dados no schema configuracao
    para verificar se existe um registro com data_execucao_modelagem como None.
    Se existir, significa que há dados importados que ainda não foram processados.
    """
    
    def __init__(
        self, 
        conn_id=None, 
        cliente_id: str = None,
        intervalo_verificacao_minutos: int = 15,
        max_tentativas: int = 48,  # Padrão de 48 tentativas (12 horas com intervalo de 15 min)
        timeout_minutos: int = 15
    ):
        """
        Inicializa o trigger de verificação de atualização do banco de dados
        
        Args:
            conn_id (str, opcional): Parâmetro mantido para compatibilidade
            cliente_id (str): Identificador do cliente
            intervalo_verificacao_minutos (int): Intervalo em minutos entre verificações
            max_tentativas (int): Número máximo de tentativas antes de falhar
            timeout_minutos (int): Tempo em minutos que deve esperar antes de uma nova tentativa
        """
        super().__init__()
        # Ignora conn_id - vamos usar DB_CONFIG_MALOKA diretamente
        self.conn_id = conn_id  # mantido para compatibilidade
        self.cliente_id = cliente_id
        self.intervalo_verificacao = intervalo_verificacao_minutos
        self.max_tentativas = max_tentativas
        self.timeout_minutos = timeout_minutos
        
    def serialize(self) -> Dict[str, Any]:
        """
        Serializa o trigger para persistência
        
        Returns:
            Dict[str, Any]: Representação serializada do trigger
        """
        return {
            "conn_id": self.conn_id,
            "cliente_id": self.cliente_id,
            "intervalo_verificacao": self.intervalo_verificacao,
            "max_tentativas": self.max_tentativas,
            "timeout_minutos": self.timeout_minutos
        }
        
    async def run(self) -> TriggerEvent:
        """
        Executa o trigger de maneira assíncrona
        
        Returns:
            TriggerEvent: Evento que indica se a execução pode prosseguir ou não
        """
        tentativas = 0
        
        while tentativas < self.max_tentativas:
            # Verifica se existe um registro com data_execucao_modelagem como None
            # Usa diretamente DB_CONFIG_MALOKA
            pode_atualizar = verificar_atualizacao_permitida(
                cliente_id=self.cliente_id,
                timeout_minutos=self.timeout_minutos
            )
            
            if pode_atualizar:
                # Retorna sucesso para que a DAG continue
                return TriggerEvent({
                    "status": "success",
                    "message": f"Cliente {self.cliente_id} possui dados importados não processados, prosseguindo com modelagens"
                })
            
            # Incrementa o contador de tentativas
            tentativas += 1
            
            # Se atingiu o número máximo de tentativas, finaliza com falha
            if tentativas >= self.max_tentativas:
                return TriggerEvent({
                    "status": "error",
                    "message": f"Excedido o número máximo de tentativas ({self.max_tentativas}) para o cliente {self.cliente_id}"
                })
                
            # Aguarda o intervalo antes da próxima verificação
            await asyncio.sleep(self.intervalo_verificacao * 60)
            
        # Se por algum motivo sair do loop sem retornar, retorna erro
        return TriggerEvent({
            "status": "error",
            "message": "Falha na verificação de atualização do banco de dados"
        })


# Funções auxiliares para uso em DAGs

def criar_deferrable_task_atualizacao_banco(
    task_id: str,
    conn_id=None,  # Mantido para compatibilidade
    cliente_id: str = None,
    intervalo_verificacao_minutos: int = 15,
    max_tentativas: int = 48,
    timeout_minutos: int = 15,
    **kwargs
):
    """
    Cria uma task deferível que verifica se o banco de dados está pronto para atualização
    
    Args:
        task_id (str): ID da tarefa no Airflow
        conn_id (str, opcional): Mantido para compatibilidade
        cliente_id (str): Identificador do cliente
        intervalo_verificacao_minutos (int): Intervalo em minutos entre verificações
        max_tentativas (int): Número máximo de tentativas antes de falhar
        timeout_minutos (int): Tempo em minutos que deve esperar antes de uma nova tentativa
        
    Returns:
        Task: Tarefa deferível configurada
    """
    # Importar apenas task decorator, não precisamos do TriggerDagRunOperator
    from airflow.decorators import task
    
    @task.branch(task_id=task_id, **kwargs)
    def verificar_atualizacao_branch(**context):
        """
        Função que verifica se existe um registro com data_execucao_modelagem como None
        e define qual branch seguir
        """
        # Usa diretamente DB_CONFIG_MALOKA
        pode_atualizar = verificar_atualizacao_permitida(
            cliente_id=cliente_id,
            timeout_minutos=timeout_minutos
        )
        
        if pode_atualizar:
            # Retorna o ID da próxima task na branch de sucesso
            print(f"Cliente {cliente_id} possui dados importados não processados, prosseguindo com modelagens")
            return context['params'].get('success_task_id')
        else:
            # Retorna o ID da task na branch de aguardar
            print(f"Cliente {cliente_id} não possui novos dados para processamento, aguardando próxima verificação")
            return context['params'].get('wait_task_id')
    
    return verificar_atualizacao_branch


def registrar_sucesso_atualizacao(conn_id=None, cliente_id: str = None, **kwargs):
    """
    Função para registrar o sucesso da atualização (execução da modelagem)
    
    Args:
        conn_id (str, opcional): Mantido para compatibilidade
        cliente_id (str): Identificador do cliente
    """
    from .database import registrar_execucao_modelagem
    
    registrar_execucao_modelagem(
        cliente_id=cliente_id,
        mensagem="Processamento concluído com sucesso"
    )


def registrar_falha_atualizacao(conn_id=None, cliente_id: str = None, **kwargs):
    """
    Função para registrar a falha na atualização
    
    Args:
        conn_id (str, opcional): Mantido para compatibilidade
        cliente_id (str): Identificador do cliente
    """
    context = kwargs.get('context', {})
    exception = context.get('exception', None)
    mensagem = str(exception) if exception else "Falha no processamento"
    
    # Não registramos falha no banco para manter a coluna data_execucao_modelagem como None
    # e permitir uma nova tentativa na próxima execução da DAG
    print(f"Falha no processamento para o cliente {cliente_id}: {mensagem}")
    print("Data de execução das modelagens não foi atualizada, permitindo nova tentativa")
