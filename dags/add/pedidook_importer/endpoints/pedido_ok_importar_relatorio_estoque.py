import os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import re
import logging
import glob
from config.db_connection import get_db_connection

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("execution_log.log"),
            logging.StreamHandler()
        ]
    )

def extract_data_estoque(path):
    match = re.search(r"estoque_analitico_(\d{8}_\d{6})", path)
    if match:
        data_estoque_str = match.group(1)
        return datetime.strptime(data_estoque_str, "%Y%m%d_%H%M%S")
    else:
        logging.error("Data e hora não encontradas no nome do arquivo de relatório.")
        raise ValueError("Data e hora não encontradas no nome do arquivo de relatório.")
    
import re

def extract_dados_aquisicao(descricao_movimento):
    dados = {}

    # Extrair operação e fornecedor
    linhas = descricao_movimento.strip().split('\n')
    if linhas and len(linhas[0].split()) >= 2:
        partes = linhas[0].split()
        dados["operacao"] = partes[0].strip().lower()
        dados["fornecedor"] = partes[1].strip().upper()

    # Padrões de extração
    padroes = {
        "novo_preco_custo": r"(?:Novo )?Preço de Custo(?: \(?Informado\)?)? R\$ ([\d.,]+)",
        "preco_custo_medio": r"(?:Novo )?Preço de Custo(?: \(?Médio\)?) R\$ ([\d.,]+)",
        "preco_custo_anterior": r"Preço de Custo Anterior R\$ ([\d.,]+)",
        "novo_preco_venda": r"(?:Novo )?Preço de Venda R\$ ([\d.,]+)",
        "preco_venda_anterior": r"Preço de Venda Anterior R\$ ([\d.,]+)",
        "novo_markup": r"(?:Novo )?Markup ([\d.,]+)%",
        "markup_anterior": r"Markup Anterior ([\d.,]+)%"
    }

    for chave, padrao in padroes.items():
        match = re.search(padrao, descricao_movimento)
        if match:
            valor = match.group(1).replace('.', '').replace(',', '.')
            dados[chave] = float(valor)

    return dados

def extract_numero_pedido(descricao_movimento):
    """
    Extrai o número do pedido ou orçamento de uma descrição de movimento 
    """
    match = re.search(r"(?:Pedido|Orçamento) (\d+)", descricao_movimento)
    if match:
        return match.group(1)
    return None

def read_and_process_excel(path):
    logging.info("Iniciando leitura do arquivo Excel.")
    df = pd.read_excel(path, header=None)
    logging.info("Leitura do arquivo Excel concluída.")
    produtos = []
    movimentos = []
    produto_atual = {}
    for i, row in df.iterrows():
        codigo = row[0]
        produto = row[1]
        if isinstance(codigo, str) and codigo.strip().isdigit():
            produto_atual = {
                'codigo_produto': codigo.strip(),
                'nome_produto': str(produto).strip(),
                'embalagem': row[6],
                'codigo_barra': row[7],
                'estoque_atual': row[8],
                'estoque_minimo': row[9],
                'validade': pd.to_datetime(row[10], errors='coerce').date() if pd.notnull(row[10]) else None
            }
            produtos.append(produto_atual.copy())
        elif isinstance(codigo, (str, datetime, pd.Timestamp)) and pd.notnull(codigo):
            data_movimento = pd.to_datetime(row[0], format='%d/%m/%y %H:%M:%S', errors='coerce')
            if pd.notnull(data_movimento):
                movimentos.append({
                    **produto_atual,
                    'data_movimento': data_movimento,
                    'tipo_movimento': row[2],
                    'quantidade': row[3],
                    'estoque_antes': row[4],
                    'estoque_depois': row[5],
                    'descricao_movimento': row[6]
                })
    produtos_df = pd.DataFrame(produtos).drop_duplicates(subset=['codigo_produto'])
    movimentos_df = pd.DataFrame(movimentos)
    produtos_df = produtos_df.where(pd.notnull(produtos_df), None)
    movimentos_df = movimentos_df.where(pd.notnull(movimentos_df), None)
    return produtos_df, movimentos_df

def insert_estoque_produtos_to_db(produtos_df, connection, data_relatorio_estoque):
    batch = []
    batch_size = 100
    total_produtos = 0

    for _, row in produtos_df.iterrows():
        batch.append((
            row['codigo_produto'],
            row['nome_produto'],
            row['embalagem'],
            row['codigo_barra'],
            row['estoque_atual'],
            row['estoque_minimo'],
            row['validade'],
            data_relatorio_estoque
        ))

        if len(batch) == batch_size:
            total_produtos += execute_batch_insert_estoque_produto(connection, batch)
            batch.clear()

    if batch:
        total_produtos += execute_batch_insert_estoque_produto(connection, batch)

    logging.info(f"Produtos inseridos/atualizados: {total_produtos}.")
    return total_produtos

def execute_batch_insert_estoque_produto(connection, batch):
    try:
        cursor = connection.cursor()
        cursor.executemany("""
            INSERT INTO pedidook.relatorio_estoque_produto (
                codigo_produto,
                nome_produto,
                embalagem,
                codigo_barra,
                estoque_atual,
                estoque_minimo,
                validade,
                data_relatorio_estoque
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (codigo_produto, data_relatorio_estoque) DO UPDATE SET
                nome_produto = EXCLUDED.nome_produto,
                embalagem = EXCLUDED.embalagem,
                codigo_barra = EXCLUDED.codigo_barra,
                estoque_atual = EXCLUDED.estoque_atual,
                estoque_minimo = EXCLUDED.estoque_minimo,
                validade = EXCLUDED.validade            

        """, batch)
        connection.commit()
        logging.info(f"Batch de {len(batch)} produtos comitado com sucesso.")
        return len(batch)
    except Exception as e:
        logging.error(f"Erro ao inserir produtos: {e}")
        raise

def insert_movimentos_to_db(movimentos_df, connection, data_relatorio_estoque):
    batch = []
    batch_size = 250
    total_movimentos = 0

    for _, row in movimentos_df.iterrows():
        numero_pedido = None
        operacao = None
        fornecedor = None
        novo_preco_custo = None
        preco_custo_medio = None
        preco_custo_anterior = None
        novo_preco_venda = None
        preco_venda_anterior = None
        novo_markup = None
        markup_anterior = None

        if isinstance(row['descricao_movimento'], str):
            numero_pedido = extract_numero_pedido(row['descricao_movimento'])

        if (
            row['tipo_movimento'] == 'Entrada'
            and isinstance(row['descricao_movimento'], str)
            and 'Custo' in row['descricao_movimento']
        ):
            dados = extract_dados_aquisicao(row['descricao_movimento'])
            operacao = dados.get('operacao')
            fornecedor = dados.get('fornecedor')
            novo_preco_custo = dados.get('novo_preco_custo')
            preco_custo_medio = dados.get('preco_custo_medio')
            preco_custo_anterior = dados.get('preco_custo_anterior')
            novo_preco_venda = dados.get('novo_preco_venda')
            preco_venda_anterior = dados.get('preco_venda_anterior')
            novo_markup = dados.get('novo_markup')
            markup_anterior = dados.get('markup_anterior')

        batch.append((
            row['codigo_produto'],
            row['data_movimento'],
            row['tipo_movimento'],
            row['quantidade'],
            row['estoque_antes'],
            row['estoque_depois'],
            row['descricao_movimento'],
            data_relatorio_estoque,
            numero_pedido,
            operacao,
            fornecedor,
            novo_preco_custo,
            preco_custo_medio,
            preco_custo_anterior,
            novo_preco_venda,
            preco_venda_anterior,
            novo_markup,
            markup_anterior
        ))

        if len(batch) == batch_size:
            total_movimentos += execute_batch_insert_movimentos(connection, batch)
            batch.clear()

    if batch:
        total_movimentos += execute_batch_insert_movimentos(connection, batch)

    logging.info(f"Movimentos inseridos/atualizados: {total_movimentos}.")
    return total_movimentos

def execute_batch_insert_movimentos(connection, batch):
    try:
        cursor = connection.cursor()
        cursor.executemany("""
            INSERT INTO pedidook.relatorio_estoque_movimento (
                codigo_produto,
                data_movimento,
                tipo_movimento,
                quantidade,
                estoque_antes,
                estoque_depois,
                descricao_movimento,
                data_relatorio_estoque,
                numero_pedido,
                operacao,
                fornecedor,
                novo_preco_custo,
                preco_custo_medio,
                preco_custo_anterior,
                novo_preco_venda,
                preco_venda_anterior,
                novo_markup,
                markup_anterior
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (codigo_produto, data_movimento, tipo_movimento) DO UPDATE SET
                quantidade = EXCLUDED.quantidade,
                estoque_antes = EXCLUDED.estoque_antes,
                estoque_depois = EXCLUDED.estoque_depois,
                descricao_movimento = EXCLUDED.descricao_movimento,
                numero_pedido = EXCLUDED.numero_pedido,
                operacao = EXCLUDED.operacao,
                fornecedor = EXCLUDED.fornecedor,
                novo_preco_custo = EXCLUDED.novo_preco_custo,
                preco_custo_medio = EXCLUDED.preco_custo_medio,
                preco_custo_anterior = EXCLUDED.preco_custo_anterior,
                novo_preco_venda = EXCLUDED.novo_preco_venda,
                preco_venda_anterior = EXCLUDED.preco_venda_anterior,
                novo_markup = EXCLUDED.novo_markup,
                markup_anterior = EXCLUDED.markup_anterior
        """, batch)
        connection.commit()
        logging.info(f"Batch de {len(batch)} movimentos comitado com sucesso.")
        return len(batch)
    except Exception as e:
        logging.error(f"Erro ao inserir movimento: {e}")
        raise

def list_estoque_files(directory):
    """
    Lista todos os arquivos na pasta especificada que seguem o padrão estoque_analitico_{data_inicio}_{data_fim}.xls,
    ignorando arquivos que já foram processados (com o sufixo '_processado' no nome).
    """
    pattern = os.path.join(directory, "estoque_analitico_*.xls")
    all_files = glob.glob(pattern)
    # Filtrar arquivos que não terminam com '_processado.xls'
    unprocessed_files = [file for file in all_files if not file.endswith("_processado.xls")]
    return unprocessed_files

def extract_data_fim_from_filename(filename):
    """
    Extrai a data de fim do nome do arquivo no formato estoque_analitico_{data_inicio}_{data_fim}.xls.
    """
    match = re.search(r"estoque_analitico_\d{8}_(\d{8})", filename)
    if match:
        data_fim_str = match.group(1)
        return datetime.strptime(data_fim_str, "%Y%m%d")
    else:
        logging.error("Data de fim não encontrada no nome do arquivo de relatório.")
        raise ValueError("Data de fim não encontrada no nome do arquivo de relatório.")

def rename_processed_file(file_path):
    """
    Renomeia o arquivo processado adicionando o sufixo '_processado_{data}_{hora}' ao nome do arquivo.
    """
    from datetime import datetime
    directory, filename = os.path.split(file_path)
    name, ext = os.path.splitext(filename)
    data_processamento = datetime.now().strftime('%Y%m%d')
    hora_processamento = datetime.now().strftime('%H%M%S')
    new_filename = f"{name}_processado_{data_processamento}_{hora_processamento}{ext}"
    new_file_path = os.path.join(directory, new_filename)
    os.rename(file_path, new_file_path)
    logging.info(f"Arquivo renomeado para: {new_file_path}")

def importar_relatorio_estoque():
    setup_logger()
    logging.info("Início da execução do script.")
    
    # Listar todos os arquivos na pasta relatorio_estoque_add
    directory = "./relatorio_estoque_add"
    files = list_estoque_files(directory)

    if not files:
        logging.warning("Nenhum arquivo encontrado na pasta relatorio_estoque_add.")
        return

    conn = get_db_connection()

    for file_path in files:
        try:
            logging.info(f"Processando arquivo: {file_path}")
            data_estoque = extract_data_fim_from_filename(file_path)
            logging.info(f"Data do estoque extraída: {data_estoque}")

            produtos_df, movimentos_df = read_and_process_excel(file_path)

            logging.info("Início da inserção dos produtos no banco de dados.")
            insert_estoque_produtos_to_db(produtos_df, conn, data_estoque)

            logging.info("Início da inserção dos movimentos e aquisições no banco de dados.")
            insert_movimentos_to_db(movimentos_df, conn, data_estoque)

            # Renomear o arquivo após o processamento
            rename_processed_file(file_path)

        except Exception as e:
            logging.error(f"Erro ao processar o arquivo {file_path}: {e}")

    conn.close()
    logging.info("Conexão com o banco de dados encerrada.")
    logging.info("Fim da execução do script.")

if __name__ == "__main__":
    importar_relatorio_estoque()
