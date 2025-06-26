import requests
import psycopg2
from psycopg2.extras import execute_values
from config.db_connection import get_db_connection
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
from decimal import Decimal

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Substitua os valores pelos seus tokens
API_URL = "https://api.pedidook.com.br/v1/produtos/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.produto (
                id_produto_maloka serial primary key,
                id_produto INT,
                excluido BOOLEAN,
                codigo TEXT,
                observacao TEXT,
                ipi FLOAT,
                peso FLOAT,
                codigo_barra TEXT,
                qtd_embalagem INT,
                categoria TEXT,
                nome TEXT,
                id_parceiro TEXT,
                validade DATE,
                estoque INT,
                marca TEXT,
                venda numeric,
                comissao numeric,
                ultima_alteracao TIMESTAMP,
                custo numeric,
                id_fornecedor INT,
                estoque_minimo INT,
                ncm TEXT,
                referencia TEXT,
                loja TEXT default 'addprincipal',
                updated_at timestamp null,
                unique (id_produto, loja)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.grade (
                id_grade_maloka serial primary key, 
                id_produto INT,
                grade TEXT,
                loja TEXT default 'addprincipal',
                unique (id_produto, grade, loja)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.historico_estoque (
                id_historico serial primary key,
                id_produto INT,
                data_estoque TIMESTAMP,
                estoque INT,
                venda FLOAT,
                custo FLOAT,
                estoque_minimo INT,
                loja TEXT default 'addprincipal',
                unique (id_produto, data_estoque, loja)
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

    produtos = data.get("produtos", [])
    if not produtos:
        logging.info("Nenhum produto encontrado na resposta da API.")
        return False

    with conn.cursor() as cur:
        produto_values = []
        ids_produtos = []
        for p in produtos:
            ids_produtos.append(p.get('id'))
            produto_values.append((
                p.get('id'),
                p.get('excluido'),
                p.get('codigo'),
                p.get('observacao'),
                p.get('ipi'),
                p.get('peso'),
                p.get('codigo_barra'),
                p.get('qtd_embalagem'),
                p.get('categoria'),
                p.get('nome'),
                p.get('id_parceiro'),
                p.get('validade'),
                p.get('estoque'),
                p.get('marca'),
                p.get('venda'),
                p.get('comissao'),
                p.get('ultima_alteracao'),
                p.get('custo'),
                p.get('id_fornecedor'),
                p.get('estoque_minimo'),
                p.get('ncm'),
                p.get('referencia'),
                datetime.now()
            ))

        insert_produtos_query = """
            INSERT INTO api.produto(
                id_produto, excluido, codigo, observacao, ipi, peso, codigo_barra, qtd_embalagem,
                categoria, nome, id_parceiro, validade, estoque, marca, venda,
                comissao, ultima_alteracao, custo, id_fornecedor, estoque_minimo,
                ncm, referencia, updated_at
            ) VALUES %s
            ON CONFLICT (id_produto, loja) DO UPDATE SET
                excluido = EXCLUDED.excluido,
                codigo = EXCLUDED.codigo,
                observacao = EXCLUDED.observacao,
                ipi = EXCLUDED.ipi,
                peso = EXCLUDED.peso,
                codigo_barra = EXCLUDED.codigo_barra,
                qtd_embalagem = EXCLUDED.qtd_embalagem,
                categoria = EXCLUDED.categoria,
                nome = EXCLUDED.nome,
                id_parceiro = EXCLUDED.id_parceiro,
                validade = EXCLUDED.validade,
                estoque = EXCLUDED.estoque,
                marca = EXCLUDED.marca,
                venda = EXCLUDED.venda,
                comissao = EXCLUDED.comissao,
                ultima_alteracao = EXCLUDED.ultima_alteracao,
                custo = EXCLUDED.custo,
                id_fornecedor = EXCLUDED.id_fornecedor,
                estoque_minimo = EXCLUDED.estoque_minimo,
                ncm = EXCLUDED.ncm,
                referencia = EXCLUDED.referencia,
                updated_at = EXCLUDED.updated_at
        """
        execute_values(cur, insert_produtos_query, produto_values)

        # Busca em lote dos últimos históricos
        cur.execute("""
            SELECT DISTINCT ON (id_produto) id_produto, estoque, venda, custo, estoque_minimo
            FROM api.historico_estoque
            WHERE id_produto = ANY(%s) AND loja = 'addprincipal'
            ORDER BY id_produto, data_estoque DESC
        """, (ids_produtos,))
        ultimos_historicos = {
            row[0]: {
                'estoque': row[1],
                'venda': row[2],
                'custo': row[3],
                'estoque_minimo': row[4],            }
            for row in cur.fetchall()
        }
        # Monta histórico somente se houver mudança
        historico_estoque_values = []
        for p in produtos:
            id_produto = p.get('id')
            estoque = int(p.get('estoque') or 0)
            venda = Decimal(str(p.get('venda') or 0))  # Using Decimal for better precision with numeric type
            custo = Decimal(str(p.get('custo') or 0))  # Using Decimal for better precision with numeric type
            estoque_minimo = int(p.get('estoque_minimo') or 0)
            historico = ultimos_historicos.get(id_produto)

            # Log para debug
            if historico:
                logging.debug(f"Produto {id_produto}: Comparando valores do histórico vs. atual")
                logging.debug(f"Estoque: {historico['estoque']} ({type(historico['estoque'])}) vs {estoque} ({type(estoque)})")
                logging.debug(f"Venda: {historico['venda']} ({type(historico['venda'])}) vs {venda} ({type(venda)})")
                logging.debug(f"Custo: {historico['custo']} ({type(historico['custo'])}) vs {custo} ({type(custo)})")
                logging.debug(f"Estoque Mínimo: {historico['estoque_minimo']} ({type(historico['estoque_minimo'])}) vs {estoque_minimo} ({type(estoque_minimo)})")

            # Garante que os valores sejam do mesmo tipo antes de comparar
            # e lida com valores None adequadamente
            if not historico or (
                historico['estoque'] != estoque or
                historico['venda'] != venda or
                historico['custo'] != custo or
                historico['estoque_minimo'] != estoque_minimo
            ):
                historico_estoque_values.append((
                    id_produto,
                    datetime.now(),
                    estoque,
                    venda,
                    custo,
                    estoque_minimo
                ))

        if historico_estoque_values:
            insert_historico_estoque_query = """
                INSERT INTO api.historico_estoque (
                    id_produto, data_estoque, estoque, venda, custo, estoque_minimo
                ) VALUES %s
                ON CONFLICT (id_produto, data_estoque, loja) DO NOTHING
            """
            execute_values(cur, insert_historico_estoque_query, historico_estoque_values)

        grade_values = []
        for p in produtos:
            for grade in p.get('grades', []):
                grade_values.append((p.get('id'), grade))

        if grade_values:
            insert_grades_query = """
                INSERT INTO api.grade (
                    id_produto, grade
                ) VALUES %s
                ON CONFLICT (id_produto, grade, loja) DO NOTHING
            """
            execute_values(cur, insert_grades_query, grade_values)

    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True


def importar_produtos():
    logging.info("Iniciando o processo de importação de produtos.")
    conn = get_db_connection()

    try:
        create_tables(conn)

        last_modified_date = '1900-01-01T00:00:00' # sempre importar todos os produtos para atualizar tabela de estoque

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de produtos concluído.")

if __name__ == "__main__":
    importar_produtos()