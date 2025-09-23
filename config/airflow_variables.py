import os
from dotenv import load_dotenv

# Tentar importar Airflow somente se estiver disponível
try:
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Forçar Airflow a não estar disponível para execução local
# AIRFLOW_AVAILABLE = False
# print(f"Airflow disponível: {AIRFLOW_AVAILABLE}")

def get_airflow_variable(var_name, default=None):
    if AIRFLOW_AVAILABLE:
        try:
            return Variable.get(var_name)
        except Exception:
            return default
        
    return default

def get_variable(var_name, default=None):
    """Busca variáveis de ambiente ou do Airflow com fallback para default"""
    if AIRFLOW_AVAILABLE:
        return get_airflow_variable(var_name, default)
    else:
        return os.getenv(var_name, default)
    
# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def get_db_config_maloka():
    """
    Retorna as configurações para conexão ao banco da Maloka,
    priorizando as variáveis do Airflow (DB_MALOKA_*) se disponíveis,
    ou as variáveis locais (DB_*) caso contrário
    """
    if AIRFLOW_AVAILABLE:
        # No ambiente Airflow, usa as variáveis DB_MALOKA_*
        return {
            'host': get_variable('DB_MALOKA_HOST'),
            'port': get_variable('DB_MALOKA_PORT'),
            'user': get_variable('DB_MALOKA_USER'),
            'password': get_variable('DB_MALOKA_PASS')
        }
    else:
        # Em ambiente local, usa as variáveis DB_*
        return {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS')
        }
    
# print(f"Airflow disponível: {AIRFLOW_AVAILABLE}")
DB_CONFIG_MALOKA = get_db_config_maloka()