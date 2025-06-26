import requests
from psycopg2.extras import execute_values
from config.db_connection import get_db_connection
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK
import logging
from dotenv import load_dotenv
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# URL da API e Headers de autenticação
API_URL = "https://api.pedidook.com.br/v1/clientes/"
HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.cliente (
                id_cliente_maloka SERIAL PRIMARY KEY,
                id_cliente INT UNIQUE,
                razao_social TEXT,
                fantasia TEXT,
                cnpj_cpf TEXT,
                limite_credito DECIMAL,
                email_copia_pedido TEXT,
                ultima_alteracao TIMESTAMP,
                excluido BOOLEAN,
                codigo TEXT,
                latitude NUMERIC,
                longitude NUMERIC,
                atendimento TEXT,
                segmento TEXT,
                email_xml_nfe TEXT,
                ponto_de_referencia TEXT,
                loja TEXT default 'addprincipal',
                unique (id_cliente, loja)            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.endereco (
                id_endereco_maloka serial4 NOT NULL,
                id_cliente int4 not NULL,
                uf text NULL,
                cidade text NULL,
                complemento text NULL,
                numero text NULL,
                logradouro text NULL,
                bairro text NULL,
                cep text NULL,
                tipo_endereco text not null,
                loja text DEFAULT 'addprincipal'::text not NULL,
                CONSTRAINT endereco_unique UNIQUE (id_cliente, tipo_endereco, loja),
                CONSTRAINT endereco_pkey PRIMARY KEY (id_endereco_maloka)
            );

        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.contato (
                id_contao_maloka SERIAL PRIMARY KEY,
                id_contato INT,
                id_cliente INT,
                nome TEXT,
                departamento TEXT,
                email TEXT,
                telefone1 TEXT,
                telefone2 TEXT,
                dia_aniversario INT,
                mes_aniversario INT,
                ultima_alteracao TIMESTAMP,
                excluido BOOLEAN,
                loja TEXT default 'addprincipal',
                unique (id_cliente, id_contato, loja)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.referencia_bancaria (
                id_referencia_bancaria_maloka SERIAL PRIMARY KEY,
                id_referencia_bancaria INT,
                id_cliente INT,
                banco TEXT,
                agencia TEXT,
                conta TEXT,
                telefone TEXT,
                gerente TEXT,
                loja TEXT default 'addprincipal',
                unique (id_cliente, id_referencia_bancaria, loja)    
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api.referencia_comercial (
                id_referencia_comercial_maloka SERIAL PRIMARY KEY,
                id_referencia_comercial INT,
                id_cliente INT,
                empresa TEXT,
                contato TEXT,
                telefone TEXT,
                loja TEXT default 'addprincipal',
                unique (id_cliente, id_referencia_comercial, loja)
            );
        """)
        conn.commit()

# Função para importar dados do API
def import_data(conn, last_modified_date, pagina=1):
    parametros = {
        'excluido': 'false',
        'alterado_apos': last_modified_date,
        'pagina': pagina
    }
    response = requests.get(API_URL, headers=HEADERS, params=parametros)
    data = response.json()

    clientes = data.get("clientes", [])
    if not clientes:
        logging.info("Nenhum cliente encontrado na resposta da API.")
        return False  # Retorna False se não houver mais clientes a serem processados

    with conn.cursor() as cur:
        cliente_values = [
            (
                c.get('id'),
                c.get('razao_social'),
                c.get('fantasia'),
                c.get('cnpj_cpf'),
                c.get('limite_credito'),
                c.get('email_copia_pedido'),
                c.get('ultima_alteracao'),
                c.get('excluido'),
                c.get('codigo'),
                c.get('latitude'),
                c.get('longitude'),
                c.get('atendimento'),
                c.get('segmento'),
                c.get('email_xml_nfe'),
                c.get('ponto_de_referencia')
            )
            for c in clientes
        ]

        insert_clientes_query = """
            INSERT INTO api.cliente (
                id_cliente, razao_social, fantasia, cnpj_cpf, limite_credito,
                email_copia_pedido, ultima_alteracao, excluido, codigo, latitude,
                longitude, atendimento, segmento, email_xml_nfe, ponto_de_referencia
            ) VALUES %s
            ON CONFLICT (id_cliente, loja) DO UPDATE SET
            razao_social = excluded.razao_social,
            fantasia = excluded.fantasia,
            cnpj_cpf = excluded.cnpj_cpf,
            limite_credito = excluded.limite_credito,
            email_copia_pedido = excluded.email_copia_pedido,
            ultima_alteracao = excluded.ultima_alteracao,
            excluido = excluded.excluido,
            codigo = excluded.codigo,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            atendimento = excluded.atendimento,
            segmento = excluded.segmento,
            email_xml_nfe = excluded.email_xml_nfe,
            ponto_de_referencia = excluded.ponto_de_referencia
        """
        execute_values(cur, insert_clientes_query, cliente_values)
    conn.commit()
    logging.info(f"Dados da página {pagina} importados com sucesso.")
    return True

def importar_clientes(last_modified_date = '1900-01-01T00:00:00'):
    logging.info("Iniciando o processo de importação de clientes.")
    conn = get_db_connection()

    try:
        create_tables(conn)

        pagina = 1
        while import_data(conn, last_modified_date, pagina):
            pagina += 1

    finally:
        conn.close()
        logging.info("Processo de importação de clientes concluído.")

if __name__ == "__main__":
    importar_clientes()