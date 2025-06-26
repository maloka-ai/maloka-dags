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
from config.db_connection import get_db_config

# Obtendo as configurações do banco de dados
DB_CONFIG = get_db_config()


# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


print(f'TOKEN_PARCEIRO: {TOKEN_PARCEIRO}')


# Substitua os valores pelos seus tokens
API_URL = "https://api.pedidook.com.br/v1/pedidos/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.pedido (
                id_pedido_maloka SERIAL PRIMARY KEY,
                id_pedido INT,
                situacao TEXT,
                base_vencimento TEXT,
                numero INT,
                observacao_cliente TEXT,
                tipo_desconto_acrescimo TEXT,
                forma_pagamento TEXT,
                transportadora_contato TEXT,
                ultima_alteracao TIMESTAMP,
                observacao_representada TEXT,
                emissao DATE,
                faturamento DATE,
                tipo_frete TEXT,
                excluido BOOLEAN,
                id_tabela_preco INT,
                id_vendedor INT,
                id_cliente INT,
                ordem_compra TEXT,
                id_parceiro INT,
                transportadora TEXT,
                previsao_entrega DATE,
                valor_frete DECIMAL,
                condicao_pagamento INT[],
                valor_desconto_acrescimo DECIMAL,
                status TEXT,
                loja TEXT default 'addprincipal',
                unique (id_pedido, loja)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.item_pedido (
                id_item_pedido SERIAL PRIMARY KEY,
                id_pedido INT,
                excluido BOOLEAN,
                preco_bruto DECIMAL,
                observacao TEXT,
                ultima_alteracao TIMESTAMP,
                qtd_embalagem INT,
                percentual_desconto_acrescimo DECIMAL,
                embalagem TEXT,
                preco_liquido DECIMAL,
                preco_custo DECIMAL,
                id_produto INT,
                quantidade INT,
                loja TEXT default 'addprincipal'
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.item_grade (
                id_item_grade SERIAL PRIMARY KEY,
                id_pedido INT,
                id_produto INT,
                tamanho_cor TEXT,
                quantidade INT
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
    data = response.json()

    pedidos = data.get("pedidos", [])
    if not pedidos:
        logging.info("Nenhum pedido encontrado na resposta da API.")
        return False  # Retorna False se não houver mais pedidos a serem processados

    with conn.cursor() as cur:
        # Excluindo itens e grades existentes relacionados ao pedido
        pedido_ids = [p.get('id') for p in pedidos]
        if pedido_ids:
            cur.execute("DELETE FROM api.item_grade WHERE id_pedido = ANY(%s)", (pedido_ids,))
            cur.execute("DELETE FROM api.item_pedido WHERE id_pedido = ANY(%s)", (pedido_ids,))

        # Inserindo pedidos
        pedido_values = [
            (
                p.get('id'),
                p.get('situacao'),
                p.get('base_vencimento'),
                p.get('numero'),
                p.get('observacao_cliente'),
                p.get('tipo_desconto_acrescimo'),
                p.get('forma_pagamento'),
                p.get('transportadora_contato'),
                p.get('ultima_alteracao'),
                p.get('observacao_representada'),
                p.get('emissao'),
                p.get('faturamento'),
                p.get('tipo_frete'),
                p.get('excluido'),
                p.get('id_tabela_preco'),
                p.get('id_vendedor'),
                p.get('id_cliente'),
                p.get('ordem_compra'),
                p.get('id_parceiro'),
                p.get('transportadora'),
                p.get('previsao_entrega'),
                p.get('valor_frete'),
                p.get('condicao_pagamento'),
                p.get('valor_desconto_acrescimo'),
                p.get('status')
            )
            for p in pedidos
        ]

        insert_pedidos_query = """
            INSERT INTO api.pedido (
                id_pedido, situacao, base_vencimento, numero, observacao_cliente, tipo_desconto_acrescimo,
                forma_pagamento, transportadora_contato, ultima_alteracao, observacao_representada, 
                emissao, faturamento, tipo_frete, excluido, id_tabela_preco, id_vendedor,
                id_cliente, ordem_compra, id_parceiro, transportadora, previsao_entrega,
                valor_frete, condicao_pagamento, valor_desconto_acrescimo, status
            ) VALUES %s
            ON CONFLICT (id_pedido, loja) DO UPDATE SET
            situacao = excluded.situacao,
            base_vencimento = excluded.base_vencimento,
            numero = excluded.numero,
            observacao_cliente = excluded.observacao_cliente,
            tipo_desconto_acrescimo = excluded.tipo_desconto_acrescimo,
            forma_pagamento = excluded.forma_pagamento,
            transportadora_contato = excluded.transportadora_contato,
            ultima_alteracao = excluded.ultima_alteracao,
            observacao_representada = excluded.observacao_representada,
            emissao = excluded.emissao,
            faturamento = excluded.faturamento,
            tipo_frete = excluded.tipo_frete,
            excluido = excluded.excluido,
            id_tabela_preco = excluded.id_tabela_preco,
            id_vendedor = excluded.id_vendedor,
            id_cliente = excluded.id_cliente,
            ordem_compra = excluded.ordem_compra,
            id_parceiro = excluded.id_parceiro,
            transportadora = excluded.transportadora,
            previsao_entrega = excluded.previsao_entrega,
            valor_frete = excluded.valor_frete,
            condicao_pagamento = excluded.condicao_pagamento,
            valor_desconto_acrescimo = excluded.valor_desconto_acrescimo,
            status = excluded.status
        """
        execute_values(cur, insert_pedidos_query, pedido_values)

        # Inserindo itens
        item_values = []
        grade_values = []

        for p in pedidos:
            pedido_id = p.get("id")
            for i in p.get("itens", []):
                item_values.append((
                    pedido_id,
                    i.get('excluido'),
                    i.get('preco_bruto'),
                    i.get('observacao'),
                    i.get('ultima_alteracao'),
                    i.get('qtd_embalagem'),
                    i.get('percentual_desconto_acrescimo'),
                    i.get('embalagem'),
                    i.get('preco_liquido'),
                    i.get('preco_custo'),
                    i.get('id_produto'),
                    i.get('quantidade')
                ))

                # Inserindo grades
                for g in i.get('grades', []):
                    grade_values.append((
                        pedido_id,
                        i.get('id_produto'),
                        g.get('tamanho_cor'),
                        g.get('quantidade')
                    ))

        insert_itens_query = """
            INSERT INTO api.item_pedido (
                id_pedido, excluido, preco_bruto, observacao, ultima_alteracao,
                qtd_embalagem, percentual_desconto_acrescimo, embalagem, preco_liquido, 
                preco_custo, id_produto, quantidade
            ) VALUES %s
            -- Assuming no conflict check needed for items as they don't have unique constraint managed in this snippet
        """
        execute_values(cur, insert_itens_query, item_values)

        insert_grades_query = """
            INSERT INTO api.item_grade (
                id_pedido, id_produto, tamanho_cor, quantidade
            ) VALUES %s
            -- Assuming no conflict check needed for grades for simplicity
        """
        if grade_values:  # Avoids executing with empty values
            execute_values(cur, insert_grades_query, grade_values)

    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def importar_pedidos(last_modified_date = '2025-04-01T00:00:00'):
    logging.info(f"Iniciando o processo de importação de pedidos desde {last_modified_date}")

    conn = get_db_connection()

    try:
        create_tables(conn)

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de pedidos concluído.")

if __name__ == "__main__":
    importar_pedidos()