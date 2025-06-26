import requests
import psycopg2
from psycopg2.extras import execute_values
from config.db_connection import get_db_connection
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


API_URL = "https://api.pedidook.com.br/v1/vendedores/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para criar a tabela de vendedores
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.vendedor (
                id_vendedor_maloka SERIAL PRIMARY KEY,
                id_vendedor INT,
                excluido BOOLEAN,
                ultima_alteracao TIMESTAMP,
                codigo_ativacao TEXT,
                nome TEXT,
                id_parceiro TEXT,
                regiao TEXT,
                informacao_adicional TEXT,
                validade_licenca DATE,
                loja TEXT default 'addprincipal',
                unique (id_vendedor,loja)
            );
        """)
        conn.commit()

# Função para importar dados da API
def import_data(conn, last_modified_date, pagina=1):
    parametros = {
        'alterado_apos': last_modified_date,
        'pagina': pagina
    }
    response = requests.get(API_URL, headers=HEADERS, params=parametros)
    data = response.json()

    vendedores = data.get("vendedores", [])
    if not vendedores:
        logging.info("Nenhum vendedor encontrado na resposta da API.")
        return False  # Retorna False se não houver mais vendedores a serem processados

    with conn.cursor() as cur:
        vendedor_values = [
            (
                v.get('id'),
                v.get('excluido'),
                v.get('ultima_alteracao'),
                v.get('codigo_ativacao'),
                v.get('nome'),
                v.get('id_parceiro'),
                v.get('regiao'),
                v.get('informacao_adicional'),
                v.get('validade_licenca')
            )
            for v in vendedores
        ]

        insert_vendedores_query = """
            INSERT INTO api.vendedor (
                id_vendedor, excluido, ultima_alteracao, codigo_ativacao, nome,
                id_parceiro, regiao, informacao_adicional, validade_licenca
            ) VALUES %s
            ON CONFLICT (id_vendedor,loja) DO UPDATE SET
            excluido = excluded.excluido,
            ultima_alteracao = excluded.ultima_alteracao,
            codigo_ativacao = excluded.codigo_ativacao,
            nome = excluded.nome,
            id_parceiro = excluded.id_parceiro,
            regiao = excluded.regiao,
            informacao_adicional = excluded.informacao_adicional,
            validade_licenca = excluded.validade_licenca;
        """
        execute_values(cur, insert_vendedores_query, vendedor_values)
    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def importar_vendedores(last_modified_date = '2025-01-01T00:00:00' ):
    logging.info("Iniciando o processo de importação de vendedores.")
    conn = get_db_connection()

    try:
        create_table(conn)

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de vendedores concluído.")

if __name__ == "__main__":
    importar_vendedores()
