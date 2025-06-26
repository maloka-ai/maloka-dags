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
API_URL_TEMPLATE = "https://api.pedidook.com.br/v1/clientes/{}/vendedores"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para obter todos os id_cliente existentes na tabela clientes
def get_client_ids(conn):
    logging.info("Obtendo IDs dos clientes do banco de dados.")
    with conn.cursor() as cur:
        cur.execute("SELECT id_cliente FROM clientes;")
        result = cur.fetchall()
        return [r[0] for r in result]

# Função para criar a tabela clientes_vendedores
def create_clientes_vendedores_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clientes_vendedores (
                id SERIAL PRIMARY KEY,
                id_cliente INT,
                id_vendedor INT UNIQUE,
                excluido BOOLEAN,
                ultima_alteracao TIMESTAMP,
                codigo_ativacao TEXT,
                nome TEXT,
                id_parceiro TEXT,
                regiao TEXT,
                informacao_adicional TEXT,
                validade_licenca DATE
            );
        """)
        conn.commit()

# Função para importar dados do API
def import_data_for_client(conn, id_cliente, alterado_apos):
    url = API_URL_TEMPLATE.format(id_cliente)
    pagina = 1
    while True:
        params = {
            'alterado_apos': alterado_apos,
            'pagina': pagina
        }
        response = requests.get(url, headers=HEADERS, params=params)
        data = response.json()

        vendedores = data.get("vendedores", [])
        if not vendedores:
            logging.info(f"Nenhum vendedor encontrado para o cliente {id_cliente} na página {pagina}.")
            break  # Sai do loop se não houver mais vendedores a serem processados

        with conn.cursor() as cur:
            # Inserindo vendedores
            vendedor_values = [
                (
                    id_cliente,
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
                INSERT INTO clientes_vendedores (
                    id_cliente, id_vendedor, excluido, ultima_alteracao, codigo_ativacao, nome,
                    id_parceiro, regiao, informacao_adicional, validade_licenca
                ) VALUES %s
                ON CONFLICT (id_vendedor) DO UPDATE SET
                    excluido = excluded.excluido,
                    ultima_alteracao = excluded.ultima_alteracao,
                    codigo_ativacao = excluded.codigo_ativacao,
                    nome = excluded.nome,
                    id_parceiro = excluded.id_parceiro,
                    regiao = excluded.regiao,
                    informacao_adicional = excluded.informacao_adicional,
                    validade_licenca = excluded.validade_licenca
            """
            execute_values(cur, insert_vendedores_query, vendedor_values)

        conn.commit()
        logging.info(f"Dados do cliente {id_cliente} na página {pagina} importados com sucesso.")
        pagina += 1
    return True

def main():
    logging.info("Iniciando o processo de importação de vendedores.")
    conn = get_db_connection()  

    try:
        create_clientes_vendedores_table(conn)
        alterado_apos = '2000-01-01T00:00:00'  # Defina uma data inicial padrão

        client_ids = get_client_ids(conn)
        for id_cliente in client_ids:
            import_data_for_client(conn, id_cliente, alterado_apos)

    finally:
        conn.close()
        logging.info("Processo de importação de vendedores concluído.")

if __name__ == "__main__":
    main()