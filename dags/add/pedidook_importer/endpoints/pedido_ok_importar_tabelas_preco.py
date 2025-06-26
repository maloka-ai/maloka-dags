import requests
import psycopg2
from psycopg2.extras import execute_values
from config.db_connection import get_db_connection
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Substitua os valores pelos seus tokens
API_URL = "https://api.pedidook.com.br/v1/tabelas_preco/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para obter a última data de modificação registrada no banco de dados
def get_last_modified_date(conn):
    logging.info("Obtendo a última data de modificação do banco de dados.")
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ultima_alteracao) FROM tabelas_preco;")
        result = cur.fetchone()
        return result[0] if result[0] else '2000-01-01T00:00:00'

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tabelas_preco (
                id SERIAL PRIMARY KEY,
                excluido BOOLEAN,
                observacao TEXT,
                ultima_alteracao TIMESTAMP,
                nome TEXT,
                id_parceiro TEXT,
                id_tabela INT UNIQUE,
                inativa BOOLEAN
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

    tabelas_preco = data.get("tabelas_preco", [])
    if not tabelas_preco:
        logging.info("Nenhuma tabela de preço encontrada na resposta da API.")
        return False  # Retorna False se não houver mais tabelas a serem processadas

    with conn.cursor() as cur:
        # Inserindo tabelas de preços
        tabela_values = [
            (
                t.get('excluido'),
                t.get('observacao'),
                t.get('ultima_alteracao'),
                t.get('nome'),
                t.get('id_parceiro'),
                t.get('id'),
                t.get('inativa')
            )
            for t in tabelas_preco
        ]

        insert_tabela_query = """
            INSERT INTO tabelas_preco (
                excluido, observacao, ultima_alteracao, nome, id_parceiro,
                id_tabela, inativa
            ) VALUES %s
            ON CONFLICT (id_tabela) DO UPDATE SET
            excluido = excluded.excluido,
            observacao = excluded.observacao,
            ultima_alteracao = excluded.ultima_alteracao,
            nome = excluded.nome,
            id_parceiro = excluded.id_parceiro,
            inativa = excluded.inativa
        """
        execute_values(cur, insert_tabela_query, tabela_values)

    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def main():
    logging.info("Iniciando o processo de importação de tabelas de preços.")
    conn = get_db_connection()

    try:
        create_tables(conn)
        last_modified_date = get_last_modified_date(conn)

        last_modified_date = '2000-01-01T00:00:00' # Use caso precise definir uma data estratégica

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de tabelas de preços concluído.")

if __name__ == "__main__":
    main()
