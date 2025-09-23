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

# Adicionar caminho para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.airflow_variables import DB_CONFIG_MALOKA

class DatabaseClient:
    """Cliente para conexão com banco de dados"""
    
    def __init__(self, conn_id_or_config=None):
        """
        Inicializa a conexão com o banco de dados
        
        Args:
            conn_id_or_config: ID da conexão no Airflow ou diretamente a configuração do banco
        """
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
        
        try:
            return pd.read_sql(query, engine, params=params)
        finally:
            engine.dispose()


def verificar_atualizacao_permitida(conn_id=None, cliente_id: str = None, timeout_minutos: int = 15) -> bool:
    """
    Verifica se é permitido realizar uma atualização para o cliente específico
    verificando a tabela log_processamento_dados no schema configuracao
    
    Verifica se o registro mais recente tem data_execucao_modelagem como None.
    Se sim, permite a atualização.
    
    Args:
        conn_id (str, opcional): Parâmetro mantido para compatibilidade
        cliente_id (str): Identificador do cliente 
        timeout_minutos (int, opcional): Tempo de espera em minutos para nova tentativa. Padrão: 15
        
    Returns:
        bool: True se a atualização for permitida, False caso contrário
    """
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA)
    
    query = """
    SELECT 
        id_log,
        data_importacao,
        data_execucao_modelagem
    FROM 
        configuracao.log_processamento_dados
    WHERE 
        cliente_id = %(cliente_id)s
    ORDER BY 
        data_importacao DESC
    LIMIT 1
    """
    
    try:
        df = db_client.execute_query(query, params={"cliente_id": cliente_id})
        
        if df.empty:
            # Se não houver registro para o cliente, não permitir atualização
            # pois não houve importação de dados
            print(f"Nenhum registro de importação encontrado para o cliente {cliente_id}")
            return False
            
        # Verificar se data_execucao_modelagem é None
        data_execucao = df['data_execucao_modelagem'].iloc[0]
        data_importacao = df['data_importacao'].iloc[0]
        
        # Se data_execucao_modelagem é None, significa que os dados foram importados mas
        # ainda não foram processados pelas modelagens
        if data_execucao is None:
            print(f"Cliente {cliente_id} tem dados importados em {data_importacao} aguardando processamento")
            return True
        else:
            print(f"Cliente {cliente_id} já tem modelagens processadas em {data_execucao} para a importação de {data_importacao}")
            return False
        
    except Exception as e:
        # Em caso de erro na consulta, logar o erro e bloquear a atualização para segurança
        print(f"Erro ao verificar status de atualização: {str(e)}")
        return False


def registrar_execucao_modelagem(conn_id=None, cliente_id: str = None, mensagem: Optional[str] = None):
    """
    Registra a data de execução da modelagem na tabela log_processamento_dados
    para o registro mais recente do cliente
    
    Args:
        conn_id (str, opcional): Parâmetro mantido para compatibilidade
        cliente_id (str): Identificador do cliente
        mensagem (str, opcional): Mensagem adicional sobre a execução
    """
    # Usa diretamente DB_CONFIG_MALOKA
    db_client = DatabaseClient(DB_CONFIG_MALOKA)
    
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
            print(f"Nenhum registro encontrado para o cliente {cliente_id}")
            return
            
        id_log = df['id_log'].iloc[0]
        
        # Atualizar o registro com a data de execução
        conn = db_client.get_connection()
        engine = create_engine(conn.get_uri())
        
        params = {
            "id_log": id_log,
            "data_execucao": datetime.now()
        }
        
        with engine.begin() as connection:
            connection.execute(text(query_update), params)
            
        print(f"Registro de execução de modelagem atualizado para o cliente {cliente_id}, id_log {id_log}")
            
    except Exception as e:
        print(f"Erro ao registrar execução de modelagem: {str(e)}")
        
        
def registrar_tentativa_atualizacao(conn_id: str, cliente_id: str, status: str, 
                                  mensagem: Optional[str] = None):
    """
    Registra uma tentativa de atualização na tabela log_processamento_dados
    Esta função está mantida por compatibilidade, mas recomenda-se usar registrar_execucao_modelagem
    
    Args:
        conn_id (str): ID da conexão do banco de dados no Airflow
        cliente_id (str): Identificador do cliente
        status (str): Status da atualização ('INICIADO', 'CONCLUIDO', 'ERRO', etc)
        mensagem (str, opcional): Mensagem adicional sobre a atualização
    """
    print(f"DEPRECATED: Use registrar_execucao_modelagem em vez de registrar_tentativa_atualizacao")
    
    # Se o status for CONCLUIDO, registrar a execução da modelagem
    if status == "CONCLUIDO":
        registrar_execucao_modelagem(conn_id, cliente_id, mensagem)
    else:
        print(f"Status {status} ignorado, apenas CONCLUIDO atualiza data_execucao_modelagem")
