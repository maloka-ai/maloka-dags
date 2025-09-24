import os
from dotenv import load_dotenv

# Tentar importar Airflow somente se estiver disponível
try:
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

def is_running_in_airflow():
    """Verifica se realmente estamos executando dentro do Airflow"""
    # Verifica variáveis de contexto de task
    airflow_context_vars = [
        'AIRFLOW_CTX_DAG_ID',
        'AIRFLOW_CTX_TASK_ID', 
        'AIRFLOW_CTX_EXECUTION_DATE'
    ]
    return any(os.getenv(var) for var in airflow_context_vars)

def get_airflow_variable_safe(var_name, default=None):
    """Busca variável do Airflow com tratamento de erro robusto"""
    if not AIRFLOW_AVAILABLE:
        return default
        
    try:
        return Variable.get(var_name, default_var=default)
    except Exception as e:
        print(f"⚠️ Erro ao acessar variável '{var_name}' no Airflow: {e}")
        return default

def get_variable(var_name, default=None):
    """
    Busca variáveis priorizando contexto real de execução
    """
    # Se Airflow disponível E executando dentro de uma task
    if AIRFLOW_AVAILABLE and is_running_in_airflow():
        return get_airflow_variable_safe(var_name, default)
    else:
        # Fallback para variáveis de ambiente
        return os.getenv(var_name, default)

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def get_db_config_maloka():
    """
    Retorna as configurações para conexão ao banco da Maloka,
    com detecção inteligente do ambiente
    """
    running_in_airflow = AIRFLOW_AVAILABLE and is_running_in_airflow()
    
    print(f"🔍 Ambiente detectado: {'Airflow Task' if running_in_airflow else 'Local/Desenvolvimento'}")
    
    if running_in_airflow:
        # No ambiente Airflow, usa as variáveis DB_MALOKA_*
        print("📡 Buscando variáveis DB_MALOKA_* no Airflow...")
        config = {
            'host': get_variable('DB_MALOKA_HOST'),
            'port': get_variable('DB_MALOKA_PORT'),
            'user': get_variable('DB_MALOKA_USER'),
            'password': get_variable('DB_MALOKA_PASS')
        }
    else:
        # Em ambiente local, usa as variáveis DB_*
        print("🏠 Buscando variáveis DB_* locais (.env)...")
        config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS')
        }
    
    # Validação
    missing = [k for k, v in config.items() if not v]
    if missing:
        env_type = "Airflow (DB_MALOKA_*)" if running_in_airflow else "Local (.env DB_*)"
        print(f"❌ Variáveis faltando em {env_type}: {missing}")
    else:
        print("✅ Todas as configurações encontradas!")
    
    return config

# Para debug - só executa se chamado diretamente
if __name__ == "__main__":
    print("🚀 Testando configuração do banco...")
    print(f"Airflow disponível: {AIRFLOW_AVAILABLE}")
    print(f"Executando em task Airflow: {is_running_in_airflow()}")
    
    config = get_db_config_maloka()
    
    print("\n📋 Configuração final:")
    for key, value in config.items():
        display_value = "***" if key == 'password' and value else value
        print(f"  {key}: {display_value}")

# Uso normal - carrega configuração
DB_CONFIG_MALOKA = get_db_config_maloka()