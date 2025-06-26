import requests
import psycopg2
from psycopg2.extras import execute_values
from config.db_connection import get_db_connection
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK
import json
import logging
from datetime import datetime
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Substitua os valores pelos seus tokens
API_URL = "https://api.pedidook.com.br/v1/condicoes_pagamento/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS condicoes_pagamento (
                id SERIAL PRIMARY KEY,
                excluido BOOLEAN,
                ultima_alteracao TIMESTAMP,
                id_condicao INT UNIQUE,
                id_parceiro TEXT,
                prazos INT[]
            );
        """)
        conn.commit()

# Função para importar dados do API
def import_data(conn, alterado_apos, pagina=1):
    parametros = {
        'alterado_apos': alterado_apos,
        'pagina': pagina
    }
    response = requests.get(API_URL, headers=HEADERS, params=parametros)
    data = response.json()

    condicoes_pagamento = data.get("condicoes_pagamento", [])
    if not condicoes_pagamento:
        logging.info("Nenhuma condição de pagamento encontrada na resposta da API.")
        return False  # Retorna False se não houver mais registros a serem processados

    with conn.cursor() as cur:
        # Inserindo condições de pagamento
        values = [
            (
                c.get('excluido'),
                c.get('ultima_alteracao'),
                c.get('id'),
                c.get('id_parceiro'),
                c.get('prazos')
            )
            for c in condicoes_pagamento
        ]

        insert_query = """
            INSERT INTO condicoes_pagamento (
                excluido, ultima_alteracao, id_condicao, id_parceiro, prazos
            ) VALUES %s
            ON CONFLICT (id_condicao) DO UPDATE SET
                excluido = excluded.excluido,
                ultima_alteracao = excluded.ultima_alteracao,
                id_parceiro = excluded.id_parceiro,
                prazos = excluded.prazos
        """
        execute_values(cur, insert_query, values)
        conn.commit()

    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def main():
    logging.info("Iniciando o processo de importação de condições de pagamento.")
    conn = get_db_connection()

    try:
        create_tables(conn)
        
        last_modified_date = '1900-01-01T00:00:00'

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de condições de pagamento concluído.")

if __name__ == "__main__":
    main()
