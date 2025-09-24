"""
Módulo para gerenciar conexões e operações com banco de dados
"""
import pandas as pd
import psycopg2
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import sys
import os
import logging

# Adicionar caminho para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Variável global para armazenar configuração
_DB_CONFIG_CACHE = None

def get_db_config():
    """
    Obtém as configurações de banco de dados com diferentes estratégias
    dependendo do ambiente de execução
    
    Returns:
        dict: Configuração de conexão ao banco de dados
    """
    global _DB_CONFIG_CACHE
    
    # Se já temos a configuração em cache, retorna
    if _DB_CONFIG_CACHE:
        return _DB_CONFIG_CACHE
    
    # Estratégia 1: Tenta importar diretamente do módulo de variáveis
    try:
        from config.airflow_variables import get_db_config_maloka
        config = get_db_config_maloka()
        if all(config.values()):  # Verifica se todos os valores estão preenchidos
            _DB_CONFIG_CACHE = config
            print("✅ DB Config obtido diretamente de airflow_variables.py")
            return config
    except Exception as e:
        print(f"⚠️ Erro ao carregar config direto: {str(e)}")
    
    # Estratégia 2: Tenta obter do Airflow via DAG executada
    try:
        from airflow.models import DagRun, TaskInstance
        from airflow.utils.db import create_session
        
        with create_session() as session:
            # Busca a última execução bem-sucedida da DAG de variáveis
            last_run = session.query(DagRun).filter(
                DagRun.dag_id == 'dag_load_variables',
                DagRun.state == 'success'
            ).order_by(DagRun.execution_date.desc()).first()
            
            if last_run:
                # Busca a task específica que carrega as variáveis
                ti = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == 'dag_load_variables',
                    TaskInstance.task_id == 'load_variables',
                    TaskInstance.run_id == last_run.run_id,
                    TaskInstance.state == 'success'
                ).first()
                
                if ti and ti.xcom_pull(task_ids='load_variables'):
                    config = ti.xcom_pull(task_ids='load_variables')
                    if all(config.values()):
                        _DB_CONFIG_CACHE = config
                        print("✅ DB Config obtido da DAG dag_load_variables via XCom")
                        return config
    except Exception as e:
        print(f"⚠️ Erro ao carregar config da DAG: {str(e)}")
    
    # Estratégia 3: Fallback para variáveis de ambiente
    try:
        config = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASS')
        }
        if all(config.values()):
            _DB_CONFIG_CACHE = config
            print("✅ DB Config obtido de variáveis de ambiente")
            return config
    except Exception as e:
        print(f"⚠️ Erro ao carregar config do ambiente: {str(e)}")
    
    # Se chegou aqui, usa config vazio (falhas serão tratadas mais adiante)
    print("❌ Não foi possível obter configuração de banco válida")
    return {'host': None, 'port': None, 'user': None, 'password': None}

# Carrega a configuração
DB_CONFIG_MALOKA = get_db_config()

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


# Exporta a função para uso em outros módulos
__all__ = ['DatabaseClient', 'get_db_config', 'registrar_execucao_modelagem', 'verificar_atualizacao_permitida', 'atualizar_todos_registros_pendentes']

class DatabaseClient:
    """Cliente para conexão com banco de dados"""
    
    def __init__(self, conn_id_or_config=None, context=None, id_cliente=None):
        """
        Inicializa a conexão com o banco de dados
        
        Args:
            conn_id_or_config: ID da conexão no Airflow ou diretamente a configuração do banco
            context: Contexto do Airflow (para logging)
            id_cliente: Identificador do cliente que será usado como nome do banco de dados
        """
        self.context = context
        self.config = None
        self.conn_id = None
        self.id_cliente = id_cliente
        
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
        # Usa o id_cliente como nome do banco se disponível, senão usa 'postgres' como padrão
        database = self.id_cliente if self.id_cliente else 'postgres'
        return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{database}"
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Executa uma query SQL e retorna o resultado como DataFrame
        
        Args:
            query (str): Query SQL a ser executada
            params (Dict[str, Any], opcional): Parâmetros para a query
            
        Returns:
            pd.DataFrame: DataFrame com o resultado da query
        """
        config = self.config
        database = self.id_cliente if self.id_cliente else 'postgres'
        query_desc = query.strip().split('\n')[0][:50] + "..." if len(query) > 50 else query
        log_info(f"Executando query: {query_desc}", self.context)
        
        try:
            # Conecta diretamente com psycopg2
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=database,
                user=config['user'],
                password=config['password']
            )
            
            # Executa a query usando pandas read_sql_query
            df = pd.read_sql_query(query, conn, params=params)
            log_info(f"Query executada com sucesso. Registros retornados: {len(df)}", self.context)
            
            # Fecha a conexão
            conn.close()
            return df
            
        except Exception as e:
            log_error(f"Erro ao executar query: {str(e)}", self.context)
            raise      
        
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
    
    # Usa diretamente DB_CONFIG_MALOKA e passa cliente_id como id_cliente
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context, id_cliente=cliente_id)
    
    # Query para verificar se existem dados não processados
    query = """
    SELECT 
        COUNT(*) as total_pendentes
    FROM 
        configuracao.log_processamento_dados
    WHERE 
        data_execucao_modelagem IS NULL
    """
    
    try:
        df = db_client.execute_query(query)
        total_pendentes = df['total_pendentes'].iloc[0] if not df.empty else 0
        
        if total_pendentes > 0:
            log_info(f"Encontrados {total_pendentes} registros com data_execucao_modelagem como None. Modelagem deve ser executada.", context)
            return True
        else:
            log_info("Nenhum registro com data_execucao_modelagem como None encontrado. Modelagem não precisa ser executada.", context)
            return False
        
    except Exception as e:
        log_error(f"Erro ao verificar registros com data_execucao_modelagem como None: {str(e)}", context)
        return False


def atualizar_todos_registros_pendentes(data_execucao=None, cliente_id=None, context=None) -> int:
    """
    Atualiza TODOS os registros da tabela log_processamento_dados com a data de execução fornecida
    ou a data atual se nenhuma for fornecida, independentemente de data_execucao_modelagem ser None ou não.
    Isso garante que todos os registros tenham a mesma data de execução mais recente.
    
    Args:
        cliente_id (str, opcional): Identificador do cliente para filtrar atualizações. Se None, atualiza todos os clientes.
        data_execucao (datetime, opcional): Data a ser usada para atualização, usa a atual se None
        context (dict, opcional): Contexto do Airflow para logging
        
    Returns:
        int: Número de registros atualizados
    """
    log_info("Atualizando todos os registros de data_execucao_modelagem", context)
    
    # Usa a data atual se nenhuma for fornecida
    if data_execucao is None:
        data_execucao = datetime.now()
        
    # Usa diretamente DB_CONFIG_MALOKA e passa cliente_id como id_cliente se fornecido
    db_client = DatabaseClient(DB_CONFIG_MALOKA, context=context, id_cliente=cliente_id)
    
    # Query base para atualizar registros
    query_base = """
    UPDATE 
        configuracao.log_processamento_dados
    SET 
        data_execucao_modelagem = %(data_execucao)s
    """
    
    # Adiciona filtro por cliente se fornecido
    query = query_base

    try:
        # Para contar quantos registros existem para atualização
        count_query_base = """
        SELECT COUNT(*) as total_registros
        FROM configuracao.log_processamento_dados
        """
        
        # Adiciona filtro por cliente se fornecido
        count_query = count_query_base
        params = {}
        
        count_df = db_client.execute_query(count_query, params=params)
        total_registros = count_df['total_registros'].iloc[0] if not count_df.empty else 0
        
        if total_registros == 0:
            log_info("Nenhum registro encontrado na tabela", context)
            return 0
            
        # Executa a atualização
        params_update = {"data_execucao": data_execucao}
            
        db_client.execute_query(query, params=params_update)
        
        log_info(f"Atualizados {total_registros} registros para o cliente com a data {data_execucao}", context)
        return total_registros
        
    except Exception as e:
        log_error(f"Erro ao atualizar registros: {str(e)}", context)
        return 0
