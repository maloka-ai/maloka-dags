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

def get_db_config_maloka(**kwargs):
    """
    Retorna as configurações para conexão ao banco da Maloka,
    com detecção inteligente do ambiente e priorização de valores no XCom
    """
    running_in_airflow = AIRFLOW_AVAILABLE and is_running_in_airflow()
    
    print(f"🔍 Ambiente detectado: {'Airflow Task' if running_in_airflow else 'Local/Desenvolvimento'}")
    
    # Primeiro, tenta obter do XCom se estiver no Airflow e tiver contexto
    if running_in_airflow and kwargs and 'ti' in kwargs:
        try:
            ti = kwargs['ti']
            print("🔄 Buscando configurações da DAG dag_load_variables via XCom...")
            xcom_config = ti.xcom_pull(task_ids='load_variables', dag_id='dag_load_variables')
            
            if xcom_config and all(k in xcom_config for k in ['host', 'port', 'user', 'password']):
                print("✅ Configurações encontradas no XCom!")
                return xcom_config
            else:
                print("⚠️ Configurações incompletas ou não encontradas no XCom, usando método padrão...")
        except Exception as e:
            print(f"⚠️ Erro ao buscar do XCom: {str(e)}")
    
    # Fallback para o método padrão
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

# Variável que vai armazenar a configuração carregada
DB_CONFIG_MALOKA = None

def get_db_config_maloka_instance(**kwargs):
    """
    Método para obter a configuração do banco da Maloka
    com contexto do Airflow (se disponível)
    """
    global DB_CONFIG_MALOKA
    
    # Se já tiver carregado e não temos contexto do Airflow, retorna o que já tem
    if DB_CONFIG_MALOKA is not None and (not kwargs or 'ti' not in kwargs):
        return DB_CONFIG_MALOKA
    
    # Se temos contexto do Airflow ou primeira carga, busca novamente
    DB_CONFIG_MALOKA = get_db_config_maloka(**kwargs)
    return DB_CONFIG_MALOKA

# Para debug - só executa se chamado diretamente
if __name__ == "__main__":
    print("🚀 Testando configuração do banco...")
    print(f"Airflow disponível: {AIRFLOW_AVAILABLE}")
    print(f"Executando em task Airflow: {is_running_in_airflow()}")
    
    config = get_db_config_maloka_instance()
    
    print("\n📋 Configuração final:")
    for key, value in config.items():
        display_value = "***" if key == 'password' and value else value
        print(f"  {key}: {display_value}")

# Inicialização padrão para uso fora do contexto de tasks
DB_CONFIG_MALOKA = get_db_config_maloka()