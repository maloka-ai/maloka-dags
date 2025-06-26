import asyncio
import aiohttp
import asyncpg
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
from config.db_connection import get_db_config
from config.settings import TOKEN_PARCEIRO, TOKEN_PEDIDOOK


# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = get_db_config()

API_URL = "https://api.pedidook.com.br/v1/pedidos/"

HEADERS = {
    'token_parceiro': TOKEN_PARCEIRO,
    'token_pedidook': TOKEN_PEDIDOOK,
    'Content-Type': 'application/json'
}

semaphore = asyncio.Semaphore(20)  # Limite de concorrência

async def create_tables(conn):
    await conn.execute("""
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
    await conn.execute("""
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
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS api.item_grade (
            id_item_grade SERIAL PRIMARY KEY,
            id_pedido INT,
            id_produto INT,
            tamanho_cor TEXT,
            quantidade INT
        );
    """)

async def fetch_pedidos(session, last_modified_date, pagina):
    params = {'alterado_apos': last_modified_date, 'pagina': pagina}
    async with semaphore:
        async with session.get(API_URL, headers=HEADERS, params=params) as resp:
            if resp.status != 200:
                logging.error(f"Erro ao buscar pedidos: {resp.status} - {await resp.text()}")
                return []
            data = await resp.json()
            return data.get("pedidos", [])

async def parse_date(date_str, as_date=False):
    """Converte string de data para objeto datetime ou date.
    Retorna None se a string for None ou inválida."""
    if not date_str:
        return None
    try:
        if 'T' in date_str:
            # ISO format with timezone
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.date() if as_date else dt
        else:
            # Simple date format YYYY-MM-DD
            return datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        logging.warning(f"Erro ao converter data: {date_str}")
        return None

async def save_pedido(pool, pedido):
    try:

        if 51491727 != pedido.get('id'):
            # Convert string timestamps to datetime objects
            ultima_alteracao = await parse_date(pedido.get('ultima_alteracao'))
            emissao = await parse_date(pedido.get('emissao'), as_date=True)
            faturamento = await parse_date(pedido.get('faturamento'), as_date=True)
            previsao_entrega = await parse_date(pedido.get('previsao_entrega'), as_date=True)
        else:
            ultima_alteracao = pedido.get('ultima_alteracao')
            emissao = pedido.get('emissao')
            faturamento = pedido.get('faturamento')
            previsao_entrega = pedido.get('previsao_entrega')
        
        async with pool.acquire() as conn:
            await conn.execute(""" INSERT INTO api.pedido (
                    id_pedido, situacao, base_vencimento, numero, observacao_cliente, tipo_desconto_acrescimo,
                    forma_pagamento, transportadora_contato, ultima_alteracao, observacao_representada, 
                    emissao, faturamento, tipo_frete, excluido, id_tabela_preco, id_vendedor,
                    id_cliente, ordem_compra, id_parceiro, transportadora, previsao_entrega,
                    valor_frete, condicao_pagamento, valor_desconto_acrescimo, status
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25
                )
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
            """,            
            pedido.get('id'),
            pedido.get('situacao'),
            pedido.get('base_vencimento'),
            pedido.get('numero'),
            pedido.get('observacao_cliente'),
            pedido.get('tipo_desconto_acrescimo'),
            pedido.get('forma_pagamento'),
            pedido.get('transportadora_contato'),
            ultima_alteracao,            
            pedido.get('observacao_representada'),
            emissao,
            faturamento,
            pedido.get('tipo_frete'),
            pedido.get('excluido'),
            pedido.get('id_tabela_preco'),
            pedido.get('id_vendedor'),
            pedido.get('id_cliente'),
            pedido.get('ordem_compra'),
            pedido.get('id_parceiro'),            
            pedido.get('transportadora'),
            previsao_entrega,
            pedido.get('valor_frete'),
            pedido.get('condicao_pagamento'),
            pedido.get('valor_desconto_acrescimo'),
            pedido.get('status')
            )
            # Exclui itens/grades antigos
            await conn.execute("DELETE FROM api.item_grade WHERE id_pedido = $1", pedido.get('id'))
            await conn.execute("DELETE FROM api.item_pedido WHERE id_pedido = $1", pedido.get('id'))                # Insere itens
            for item in pedido.get('itens', []):                # Convert string timestamp to datetime object for item
                item_ultima_alteracao = await parse_date(item.get('ultima_alteracao'))
                
                await conn.execute("""
                    INSERT INTO api.item_pedido (
                        id_pedido, excluido, preco_bruto, observacao, ultima_alteracao,
                        qtd_embalagem, percentual_desconto_acrescimo, embalagem, preco_liquido, 
                        preco_custo, id_produto, quantidade
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                    )
                """,
                pedido.get('id'),
                item.get('excluido'),
                item.get('preco_bruto'),
                item.get('observacao'),
                item_ultima_alteracao,
                item.get('qtd_embalagem'),
                item.get('percentual_desconto_acrescimo'),
                item.get('embalagem'),
                item.get('preco_liquido'),
                item.get('preco_custo'),
                item.get('id_produto'),
                item.get('quantidade')
                )
                # Insere grades
                for grade in item.get('grades', []):
                    await conn.execute("""
                        INSERT INTO api.item_grade (
                            id_pedido, id_produto, tamanho_cor, quantidade
                        ) VALUES ($1, $2, $3, $4)
                    """,
                    pedido.get('id'),
                    item.get('id_produto'),
                    grade.get('tamanho_cor'),
                    grade.get('quantidade')
                    )
            logging.info(f"Pedido {pedido.get('id')} importado com sucesso.")
    except Exception as e:
        logging.error(f"Erro: {str(e)} ao importar pedido: {pedido} ")

async def process_pagina(session, pool, last_modified_date, pagina):
    pedidos = await fetch_pedidos(session, last_modified_date, pagina)
    if not pedidos:
        return False
    tasks = [save_pedido(pool, pedido) for pedido in pedidos]
    await asyncio.gather(*tasks)
    return True

async def main(last_modified_date='2025-06-01T00:00:00'):
    logging.info(f"Iniciando importação assíncrona de pedidos desde {last_modified_date}")
    pool = await asyncpg.create_pool(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        min_size=5,
        max_size=20
    )
    try:
        async with pool.acquire() as conn:
            await create_tables(conn)
        async with aiohttp.ClientSession() as session:
            pagina = 1
            while True:
                ok = await process_pagina(session, pool, last_modified_date, pagina)
                if not ok:
                    break
                pagina += 1
    finally:
        await pool.close()
        logging.info("Processo de importação assíncrona de pedidos concluído.")

if __name__ == "__main__":
    asyncio.run(main())
