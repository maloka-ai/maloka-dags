import os
import psycopg2
from config.settings import get_db_config

DB_CONFIG = get_db_config()

def get_db_connection():
    """
    Cria e retorna uma conexão com o banco de dados usando as configurações fornecidas.
    """
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )

def execute_migration():
    """
    Executa a migração de dados para o core.
    """
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT api.migrar_dados_api_to_maloka_core();")
            conn.commit()
            print("Migração de dados concluída com sucesso.")
    except Exception as e:
        print(f"Erro ao executar a migração de dados: {e}")
    finally:
        conn.close()
