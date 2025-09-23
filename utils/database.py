"""
Módulo para gerenciar conexões e operações com banco de dados
"""
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import sys
import os
import logging

# Adicionar caminho para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.airflow_variables import DB_CONFIG_MALOKA

# Tenta importar o sistema de logging do Airflow
try:
    from utils.airflow_logging import configurar_logger, log_task_info
    AIRFLOW_LOGGER_DISPONIVEL = True
except ImportError:
    AIRFLOW_LOGGER_DISPONIVEL = False

# Configura o logger
logger = logging.getLogger(__name__)

# Funções auxiliares para logging
def log_info(mensagem, context=None):
    """Registra uma mensagem de log informativa"""
    if AIRFLOW_LOGGER_DISPONIVEL and context:
        log_task_info(context, mensagem, nivel="info")
    else:
        logger.info(mensagem)
        print(mensagem)

def log_warning(mensagem, context=None):
    """Registra uma mensagem de log de aviso"""
    if AIRFLOW_LOGGER_DISPONIVEL and context:
        log_task_info(context, mensagem, nivel="warning")
    else:
        logger.warning(mensagem)
        print(f"AVISO: {mensagem}")

def log_error(mensagem, context=None):
    """Registra uma mensagem de log de erro"""
    if AIRFLOW_LOGGER_DISPONIVEL and context:
        log_task_info(context, mensagem, nivel="error")
    else:
        logger.error(mensagem)
        print(f"ERRO: {mensagem}")


class DatabaseClient:
    """Cliente para conexão com banco de dados"""
    
    def __init__(self, conn_id_or_config=None, context=None):
        """
        Inicializa a conexão com o banco de dados
        
        Args:
            conn_id_or_config: ID da conexão no Airflow ou diretamente a configuração do banco
            context: Contexto do Airflow (para logging)
        """
        self.context = context
        self.config = None
        self.conn_id = None
        
        # Se recebeu uma configuração direta (dict)
        if isinstance(conn_id_or_config, dict):
            self.config = conn_id_or_config
        elif conn_id_or_config is None:
            # Usa a configuração padrão do DB_CONFIG_MALOKA
            self.config = DB_CONFIG_MALOKA
        else:
            # Para compatibilidade com código existente que passa conn_id
            self.conn_id = conn_id_or_config
            self.config = DB_CONFIG_MALOKA
        
    def get_connection_uri(self):
        """
        Obtém a URI para conexão com o banco de dados
        
        Returns:
            str: URI de conexão com o banco de dados
        """
        config = self.config
        return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/postgres"
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Executa uma query SQL e retorna o resultado como DataFrame
        
        Args:
            query (str): Query SQL a ser executada
            params (Dict[str, Any], opcional): Parâmetros para a query
            
        Returns:
            pd.DataFrame: DataFrame com o resultado da query
        """
        engine = create_engine(self.get_connection_uri())
        query_desc = query.strip().split('\n')[0][:50] + "..." if len(query) > 50 else query
        log_info(f"Executando query: {query_desc}", self.context)
        
        try:
            df = pd.read_sql(query, engine, params=params)
            log_info(f"Query executada com sucesso. Registros retornados: {len(df)}", self.context)
            return df
        except Exception as e:
            log_error(f"Erro ao executar query: {str(e)}", self.context)
            raise
        finally:
            engine.dispose()


def atualizar_status_processamento(cliente_id: str, context=None) -> bool:
    """
    Atualiza o status de processamento para o cliente, marcando data_execucao_modelagem com a data/hora atual
    
    Args:
        cliente_id (str): Identificador do cliente
        context (dict, opcional): Contexto do Airflow para logging
        
    Returns:
        bool: True se a atualização foi bem-sucedida, False caso contrário
    """
    log_info(f"Atualizando status de processamento para o cliente {cliente_id}", context)
    
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context)
    
    # Query para buscar o último registro com data_execucao_modelagem nula
    select_query = """
    SELECT 
        id_log 
    FROM 
        configuracao.log_processamento_dados
    WHERE 
        cliente_id = %(cliente_id)s
        AND data_execucao_modelagem IS NULL
    ORDER BY 
        data_importacao DESC
    LIMIT 1
    """
    
    try:
        df = db_client.execute_query(select_query, params={"cliente_id": cliente_id})
        
        if df.empty:
            log_warning(f"Nenhum registro de importação pendente encontrado para o cliente {cliente_id}", context)
            return False
            
        id_log = df['id_log'].iloc[0]
        
        # Query para atualizar o registro
        update_query = """
        UPDATE 
            configuracao.log_processamento_dados
        SET 
            data_execucao_modelagem = CURRENT_TIMESTAMP
        WHERE 
            id_log = %(id_log)s
        """
        
        db_client.execute_query(update_query, params={"id_log": id_log})
        log_info(f"Status de processamento atualizado com sucesso para o cliente {cliente_id} (log_id: {id_log})", context)
        return True
        
    except Exception as e:
        log_error(f"Erro ao atualizar status de processamento: {str(e)}", context)
        return False


def registrar_execucao_modelagem(conn_id=None, cliente_id: str = None, mensagem: Optional[str] = None, context=None):
    """
    Registra a data de execução da modelagem na tabela log_processamento_dados
    para o registro mais recente do cliente
    
    Args:
        conn_id (str, opcional): Parâmetro mantido para compatibilidade
        cliente_id (str): Identificador do cliente
        mensagem (str, opcional): Mensagem adicional sobre a execução
        context (dict, opcional): Contexto do Airflow para logging
    """
    log_info(f"Registrando execução de modelagem para o cliente {cliente_id}", context)
    
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context)
    
    # Consulta para obter o id_log mais recente
    query_select = """
    SELECT id_log
    FROM configuracao.log_processamento_dados
    WHERE cliente_id = %(cliente_id)s
    ORDER BY data_importacao DESC
    LIMIT 1
    """
    
    # Query para atualizar o registro com a data de execução
    query_update = """
    UPDATE configuracao.log_processamento_dados
    SET data_execucao_modelagem = %(data_execucao)s
    WHERE id_log = %(id_log)s
    """
    
    try:
        from sqlalchemy import create_engine, text
        
        # Buscar o ID do registro mais recente
        df = db_client.execute_query(query_select, params={"cliente_id": cliente_id})
        
        if df.empty:
            log_warning(f"Nenhum registro encontrado para o cliente {cliente_id}", context)
            return
            
        id_log = df['id_log'].iloc[0]
        
        # Atualizar o registro com a data de execução
        log_info(f"Atualizando registro id_log {id_log} com data de execução atual", context)
        current_time = datetime.now()
        
        db_client.execute_query(query_update, params={
            "id_log": id_log,
            "data_execucao": current_time
        })
        
        log_info(f"Registro de execução de modelagem atualizado para o cliente {cliente_id}, id_log {id_log} com timestamp {current_time}", context)
            
    except Exception as e:
        log_error(f"Erro ao registrar execução de modelagem: {str(e)}", context)
        
        
def verificar_atualizacao_permitida(cliente_id: str, timeout_minutos: int = 15, context=None) -> bool:
    """
    Verifica se existe um registro com data_execucao_modelagem como None para o cliente especificado
    e que tenha sido importado há pelo menos timeout_minutos.
    
    Args:
        cliente_id (str): Identificador do cliente
        timeout_minutos (int): Tempo mínimo em minutos desde a importação para permitir processamento
        context (dict, opcional): Contexto do Airflow para logging
        
    Returns:
        bool: True se existir um registro que pode ser processado, False caso contrário
    """
    log_info(f"Verificando se cliente {cliente_id} possui dados não processados e importados há mais de {timeout_minutos} minutos", context)
    
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context)
    
    # Query para verificar se existem dados não processados
    query = """
    SELECT 
        id_log,
        data_importacao
    FROM 
        configuracao.log_processamento_dados
    WHERE 
        cliente_id = %(cliente_id)s
        AND data_execucao_modelagem IS NULL
        AND data_importacao < (CURRENT_TIMESTAMP - INTERVAL '%(timeout_minutos)s minutes')
    ORDER BY 
        data_importacao DESC
    LIMIT 1
    """
    
    try:
        df = db_client.execute_query(query, params={
            "cliente_id": cliente_id,
            "timeout_minutos": timeout_minutos
        })
        
        if df.empty:
            log_info(f"Nenhum registro pendente de processamento encontrado para o cliente {cliente_id}", context)
            return False
            
        log_info(f"Encontrado registro com data_importacao = {df['data_importacao'].iloc[0]} para o cliente {cliente_id}, pronto para processamento", context)
        return True
        
    except Exception as e:
        log_error(f"Erro ao verificar registros pendentes: {str(e)}", context)
        return False


def verificar_e_processar_registros_pendentes(context=None) -> Dict[str, int]:
    """
    Verifica todos os registros com data_execucao_modelagem nulo e retorna um dicionário
    com os clientes que possuem registros pendentes.
    
    Args:
        context (dict, opcional): Contexto do Airflow para logging
        
    Returns:
        Dict[str, int]: Dicionário com cliente_id como chave e quantidade de registros pendentes como valor
    """
    log_info("Verificando todos os registros pendentes de processamento", context)
    
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context)
    
    # Query para buscar todos os clientes com registros pendentes
    query = """
    SELECT 
        cliente_id,
        COUNT(*) as registros_pendentes
    FROM 
        configuracao.log_processamento_dados
    WHERE 
        data_execucao_modelagem IS NULL
    GROUP BY 
        cliente_id
    ORDER BY 
        cliente_id
    """
    
    try:
        df = db_client.execute_query(query)
        
        if df.empty:
            log_info("Nenhum registro pendente de processamento encontrado", context)
            return {}
            
        # Converte o DataFrame para um dicionário
        clientes_pendentes = df.set_index('cliente_id')['registros_pendentes'].to_dict()
        
        log_info(f"Encontrados {len(clientes_pendentes)} clientes com registros pendentes: {clientes_pendentes}", context)
        return clientes_pendentes
        
    except Exception as e:
        log_error(f"Erro ao verificar registros pendentes: {str(e)}", context)
        return {}


def atualizar_todos_registros_pendentes(data_execucao=None, context=None) -> int:
    """
    Atualiza todos os registros com data_execucao_modelagem nulo para a data especificada
    ou a data atual se nenhuma for fornecida.
    
    Args:
        data_execucao (datetime, opcional): Data a ser usada para atualização, usa a atual se None
        context (dict, opcional): Contexto do Airflow para logging
        
    Returns:
        int: Número de registros atualizados
    """
    log_info("Atualizando todos os registros pendentes de processamento", context)
    
    # Usa a data atual se nenhuma for fornecida
    if data_execucao is None:
        data_execucao = datetime.now()
        
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context)
    
    # Query para atualizar todos os registros pendentes
    query = """
    UPDATE 
        configuracao.log_processamento_dados
    SET 
        data_execucao_modelagem = %(data_execucao)s
    WHERE 
        data_execucao_modelagem IS NULL
    """
    
    try:
        # Para contar quantos registros foram atualizados, primeiro contamos os pendentes
        count_query = """
        SELECT COUNT(*) as total_pendentes
        FROM configuracao.log_processamento_dados
        WHERE data_execucao_modelagem IS NULL
        """
        
        count_df = db_client.execute_query(count_query)
        total_pendentes = count_df['total_pendentes'].iloc[0] if not count_df.empty else 0
        
        if total_pendentes == 0:
            log_info("Nenhum registro pendente para atualização", context)
            return 0
            
        # Executa a atualização
        db_client.execute_query(query, params={"data_execucao": data_execucao})
        
        log_info(f"Atualizados {total_pendentes} registros pendentes com a data {data_execucao}", context)
        return total_pendentes
        
    except Exception as e:
        log_error(f"Erro ao atualizar registros pendentes: {str(e)}", context)
        return 0


def registrar_tentativa_atualizacao(conn_id: str, cliente_id: str, status: str, 
                                  mensagem: Optional[str] = None, context=None):
    """
    Registra uma tentativa de atualização na tabela log_processamento_dados
    Esta função está mantida por compatibilidade, mas recomenda-se usar registrar_execucao_modelagem
    
    Args:
        conn_id (str): ID da conexão do banco de dados no Airflow
        cliente_id (str): Identificador do cliente
        status (str): Status da atualização ('INICIADO', 'CONCLUIDO', 'ERRO', etc)
        mensagem (str, opcional): Mensagem adicional sobre a atualização
        context (dict, opcional): Contexto do Airflow para logging
    """
    log_warning(f"DEPRECATED: Use registrar_execucao_modelagem em vez de registrar_tentativa_atualizacao", context)
    
    # Se o status for CONCLUIDO, registrar a execução da modelagem
    if status == "CONCLUIDO":
        registrar_execucao_modelagem(conn_id, cliente_id, mensagem, context)
    else:
        log_info(f"Status {status} ignorado, apenas CONCLUIDO atualiza data_execucao_modelagem", context)
