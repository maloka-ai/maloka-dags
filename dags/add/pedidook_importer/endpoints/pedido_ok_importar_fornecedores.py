import requests
import psycopg2
from psycopg2.extras import execute_values
from config.db_connection import get_db_connection
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK
import json
import logging
from dotenv import load_dotenv
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Substitua os valores pelos seus tokens
API_URL = "https://api.pedidook.com.br/v1/fornecedores/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.fornecedor (
                id_fornecedor_maloka SERIAL PRIMARY KEY,
                id_fornecedor INT,
                excluido BOOLEAN,
                telefone TEXT,
                observacao TEXT,
                uf TEXT,
                cidade TEXT,
                complemento TEXT,
                numero TEXT,
                logradouro TEXT,
                bairro TEXT,
                cep TEXT,
                id_parceiro TEXT UNIQUE,
                ie_rg TEXT,
                fantasia TEXT,
                site TEXT,
                ultima_alteracao TIMESTAMP,
                razao_social TEXT,
                ponto_de_referencia TEXT,
                celular TEXT,
                cnpj_cpf TEXT,
                contato TEXT,
                email TEXT,
                loja TEXT default 'addprincipal',
                unique (id_fornecedor, loja)
            );
        """)
        conn.commit()

# Função para importar dados do API
def import_data(conn, last_modified_date, pagina=1):
    parametros = {
        'alterado_apos': last_modified_date,
        'pagina': pagina
    }

    response = requests.get(API_URL, headers=HEADERS, params=parametros)
    response.raise_for_status()  # Raise an error for bad responses
    data = response.json()

    fornecedores = data.get("fornecedores", [])
    if not fornecedores:
        logging.info("Nenhum fornecedor encontrado na resposta da API.")
        return False  # Retorna False se não houver mais fornecedores a serem processados

    with conn.cursor() as cur:
        fornecedor_values = [
            (
                f.get('id'),
                f.get('excluido'),
                f.get('telefone'),
                f.get('observacao'),
                f['endereco'].get('uf'),
                f['endereco'].get('cidade'),
                f['endereco'].get('complemento'),
                f['endereco'].get('numero'),
                f['endereco'].get('logradouro'),
                f['endereco'].get('bairro'),
                f['endereco'].get('cep'),
                f.get('id_parceiro'),
                f.get('ie_rg'),
                f.get('fantasia'),
                f.get('site'),
                f.get('ultima_alteracao'),
                f.get('razao_social'),
                f.get('ponto_de_referencia'),
                f.get('celular'),
                f.get('cnpj_cpf'),
                f.get('contato'),
                f.get('email')
            )
            for f in fornecedores
        ]

        insert_fornecedores_query = """
            INSERT INTO api.fornecedor (
                id_fornecedor, excluido, telefone, observacao, uf, cidade, complemento, numero, logradouro, bairro, cep,
                id_parceiro, ie_rg, fantasia, site, ultima_alteracao, razao_social, ponto_de_referencia,
                celular, cnpj_cpf, contato, email
            ) VALUES %s
            ON CONFLICT (id_fornecedor, loja) DO UPDATE SET
                excluido = excluded.excluido,
                telefone = excluded.telefone,
                observacao = excluded.observacao,
                uf = excluded.uf,
                cidade = excluded.cidade,
                complemento = excluded.complemento,
                numero = excluded.numero,
                logradouro = excluded.logradouro,
                bairro = excluded.bairro,
                cep = excluded.cep,
                id_parceiro = excluded.id_parceiro,
                ie_rg = excluded.ie_rg,
                fantasia = excluded.fantasia,
                site = excluded.site,
                ultima_alteracao = excluded.ultima_alteracao,
                razao_social = excluded.razao_social,
                ponto_de_referencia = excluded.ponto_de_referencia,
                celular = excluded.celular,
                cnpj_cpf = excluded.cnpj_cpf,
                contato = excluded.contato,
                email = excluded.email;
        """
        execute_values(cur, insert_fornecedores_query, fornecedor_values)
    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def importar_fornecedores(last_modified_date = '2025-01-01T00:00:00'):
    logging.info("Iniciando o processo de importação de fornecedores.")
    conn = get_db_connection()  

    try:
        create_tables(conn)

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de fornecedores concluído.")

if __name__ == "__main__":
    importar_fornecedores()