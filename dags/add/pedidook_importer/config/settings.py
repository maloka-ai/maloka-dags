import os
from dotenv import load_dotenv

# Tentar importar Airflow somente se estiver disponível
try:
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def get_airflow_variable(var_name, default=None):
    if AIRFLOW_AVAILABLE:
        try:
            return Variable.get(var_name)
        except Exception:
            return default


def get_variable(var_name, default=None):
    """Busca variáveis de ambiente ou do Airflow com fallback para default"""
    if AIRFLOW_AVAILABLE:
        return get_airflow_variable(var_name, default)
    else:
        return os.getenv(var_name, default)
    

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Função para obter as configurações do banco de dados
def get_db_config():
    return {
        'host': get_variable('ADD_DB_HOST'),
        'database': get_variable('ADD_DB_NAME'),
        'user': get_variable('ADD_DB_USER'),
        'password': get_variable('ADD_DB_PASSWORD')
    }

TOKEN_PARCEIRO = get_variable('ADD_TOKEN_PARCEIRO')
TOKEN_PEDIDOOK = get_variable('ADD_TOKEN_PEDIDOOK')

# Print das variáveis para verificação
print('ADD_DB_HOST:', get_variable('ADD_DB_HOST'))
print('ADD_DB_NAME:', get_variable('ADD_DB_NAME'))
print('ADD_DB_USER:', get_variable('ADD_DB_USER'))
print('ADD_DB_PASSWORD:', get_variable('ADD_DB_PASSWORD'))
print('ADD_TOKEN_PARCEIRO:', TOKEN_PARCEIRO)
print('ADD_TOKEN_PEDIDOOK:', TOKEN_PEDIDOOK)
