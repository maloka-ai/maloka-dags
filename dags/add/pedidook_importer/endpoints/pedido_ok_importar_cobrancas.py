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

# Configuração do API
API_URL = "https://api.pedidook.com.br/v1/cobrancas/"

HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para obter a última data de modificação registrada no banco de dados
def get_last_modified_date(conn):
    logging.info("Obtendo a última data de modificação do banco de dados.")
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ultima_alteracao) FROM cobrancas;")
        result = cur.fetchone()
        return result[0] if result[0] else '2000-01-01T00:00:00'

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cobrancas (
                id SERIAL PRIMARY KEY,
                referente TEXT,
                excluido BOOLEAN,
                observacao TEXT,
                id_cliente INT,
                acrescimo DECIMAL,
                desconto DECIMAL,
                detalhe_forma_pagamento TEXT,
                valor_pago DECIMAL,
                percentual_juros_cobrado DECIMAL,
                valor DECIMAL,
                id_parceiro TEXT,
                vencimento DATE,
                percentual_multa DECIMAL,
                valor_multa DECIMAL,
                forma_pagamento TEXT,
                dias_juros_cobrado INT,
                percentual_juros_mensal DECIMAL,
                ultima_alteracao TIMESTAMP,
                id_cobranca INT UNIQUE,
                pagamento DATE,
                id_pedido INT,
                valor_juros DECIMAL
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

    cobrancas = data.get("cobrancas", [])
    if not cobrancas:
        logging.info("Nenhuma cobrança encontrada na resposta da API.")
        return False  # Retorna False se não houver mais cobranças a serem processadas

    with conn.cursor() as cur:
        # Inserindo cobranças
        cobranca_values = [
            (
                c.get('referente'),
                c.get('excluido'),
                c.get('observacao'),
                c.get('id_cliente'),
                c.get('acrescimo'),
                c.get('desconto'),
                c.get('detalhe_forma_pagamento'),
                c.get('valor_pago'),
                c.get('percentual_juros_cobrado'),
                c.get('valor'),
                c.get('id_parceiro'),
                c.get('vencimento'),
                c.get('percentual_multa'),
                c.get('valor_multa'),
                c.get('forma_pagamento'),
                c.get('dias_juros_cobrado'),
                c.get('percentual_juros_mensal'),
                c.get('ultima_alteracao'),
                c.get('id'),
                c.get('pagamento'),
                c.get('id_pedido'),
                c.get('valor_juros')
            )
            for c in cobrancas
        ]

        insert_cobrancas_query = """
            INSERT INTO cobrancas (
                referente, excluido, observacao, id_cliente, acrescimo, desconto, 
                detalhe_forma_pagamento, valor_pago, percentual_juros_cobrado, valor, id_parceiro, 
                vencimento, percentual_multa, valor_multa, forma_pagamento, dias_juros_cobrado, 
                percentual_juros_mensal, ultima_alteracao, id_cobranca, pagamento, id_pedido, valor_juros
            ) VALUES %s
            ON CONFLICT (id_cobranca) DO UPDATE SET
            referente = excluded.referente,
            excluido = excluded.excluido,
            observacao = excluded.observacao,
            id_cliente = excluded.id_cliente,
            acrescimo = excluded.acrescimo,
            desconto = excluded.desconto,
            detalhe_forma_pagamento = excluded.detalhe_forma_pagamento,
            valor_pago = excluded.valor_pago,
            percentual_juros_cobrado = excluded.percentual_juros_cobrado,
            valor = excluded.valor,
            id_parceiro = excluded.id_parceiro,
            vencimento = excluded.vencimento,
            percentual_multa = excluded.percentual_multa,
            valor_multa = excluded.valor_multa,
            forma_pagamento = excluded.forma_pagamento,
            dias_juros_cobrado = excluded.dias_juros_cobrado,
            percentual_juros_mensal = excluded.percentual_juros_mensal,
            ultima_alteracao = excluded.ultima_alteracao,
            pagamento = excluded.pagamento,
            id_pedido = excluded.id_pedido,
            valor_juros = excluded.valor_juros
        """
        execute_values(cur, insert_cobrancas_query, cobranca_values)
    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def main():
    logging.info("Iniciando o processo de importação de cobranças.")
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
        logging.info("Processo de importação de cobranças concluído.")

if __name__ == "__main__":
    main()
