"""
Módulo para análise de orçamentos em um sistema de vendas.

Este módulo contém funções para processar dados de orçamentos e pedidos,
calcular métricas de conversão e gerar relatórios de análise para clientes.
"""
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Union
import os
import warnings
import traceback
from multiprocessing import Pool, cpu_count
import sys

import pandas as pd
import numpy as np
import psycopg2

# Adiciona o caminho raiz ao PYTHONPATH para importar módulos do projeto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import get_db_config_maloka_instance

# Obtém a configuração inicial do banco
DB_CONFIG_MALOKA = get_db_config_maloka_instance()

# Suprime avisos que podem interferir na saída
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def processar_lote(args: Tuple[List[Tuple], Dict[str, Any], str, int]) -> Tuple[int, Optional[str]]:
    """
    Processa um lote de dados em paralelo para inserção no banco de dados.

    Args:
        args: Tupla contendo:
            - lote_dados: Lista de tuplas com os dados a serem inseridos
            - db_config: Configurações da conexão com o banco de dados
            - query: Query SQL para inserção
            - start_index: Índice inicial do lote (para rastreamento)

    Returns:
        Tupla contendo:
            - Número de registros inseridos
            - Mensagem de erro (None se não houver erro)
    """
    lote_dados, db_config, query, start_index = args
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Otimizar para inserção
        cursor.execute("SET synchronous_commit = off")
        
        # Inserir dados
        cursor.executemany(query, lote_dados)
        conn.commit()
        
        # Fechar conexão
        cursor.close()
        conn.close()
        
        return len(lote_dados), None  # Retorna quantidade inserida e sem erro
    except Exception as e:
        return 0, str(e)  # Retorna 0 registros inseridos e a mensagem de erro

def carregar_dados_cliente(
    nome_cliente: str
) -> Dict[str, Union[pd.DataFrame, None]]:
    """
    Carrega os dados básicos do cliente do banco de dados.
    
    Args:
        nome_cliente: Nome do cliente para o qual carregar os dados
        
    Returns:
        Dicionário contendo os DataFrames carregados ou None se ocorrer erro
    """
    # Verificar se o cliente existe na configuração
    if nome_cliente not in CLIENTES:
        print(f"Erro: Cliente '{nome_cliente}' não encontrado na configuração!")
        print(f"Clientes disponíveis: {', '.join(CLIENTES.keys())}")
        return {}
    
    # Carregar configurações do cliente
    config_cliente = CLIENTES[nome_cliente]
    database = config_cliente["database"]
    schema = config_cliente["schema"]
    
    print(f"Gerando relatórios para o cliente: {nome_cliente}")
    print(f"Database: {database}, Schema: {schema}")
    
    # Dicionário para armazenar os DataFrames
    dataframes = {}
    
    # Configuração da conexão
    try:
        # Conectar ao PostgreSQL
        print("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_CONFIG_MALOKA['host'],
            database=database,
            user=DB_CONFIG_MALOKA['user'],
            password=DB_CONFIG_MALOKA['password'],
            port=DB_CONFIG_MALOKA['port']
        )
        
        print("Conexão estabelecida com sucesso!")
        
        # Carregar tabela vendas
        print("Consultando a tabela vendas...")
        query = f"SELECT * FROM {schema}.venda"
        df_vendas = pd.read_sql_query(query, conn)
        df_vendas['id_venda'] = df_vendas['id_venda'].astype('int64')
        dataframes['vendas'] = df_vendas
        
        # Carregar tabela clientes
        print("Consultando a tabela cliente...")
        query = f"SELECT * FROM {schema}.cliente"
        df_clientes = pd.read_sql_query(query, conn)
        dataframes['clientes'] = df_clientes
        
        # Carregar tabela venda_item
        print("Consultando a tabela venda_item...")
        query = f"SELECT * FROM {schema}.venda_item"
        df_venda_itens = pd.read_sql_query(query, conn)
        df_venda_itens['id_venda'] = df_venda_itens['id_venda'].astype('int64')
        dataframes['venda_itens'] = df_venda_itens
        
        # Carregar tabela loja
        print("Consultando a tabela loja...")
        query = f"SELECT * FROM {schema}.loja"
        df_lojas = pd.read_sql_query(query, conn)
        dataframes['lojas'] = df_lojas
        
        # Carregar tabela categoria
        print("Consultando a tabela categoria...")
        query = f"SELECT * FROM {schema}.categoria"
        df_categorias = pd.read_sql_query(query, conn)
        dataframes['categorias'] = df_categorias
        
        # Carregar tabela produto
        print("Consultando a tabela produto...")
        query = f"SELECT id_produto, id_categoria FROM {schema}.produto"
        df_produtos = pd.read_sql_query(query, conn)
        dataframes['produtos'] = df_produtos
        
        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")
        
        # Adicionar informações do cliente ao dicionário
        dataframes['config'] = {
            'database': database,
            'schema': schema,
            'nome_cliente': nome_cliente
        }
        
        return dataframes

    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")
        print("4. O esquema e as tabelas existem para este cliente")
        return {}


def preparar_dados_orcamento_pedido(
    dataframes: Dict[str, Union[pd.DataFrame, Dict[str, str]]]
) -> Dict[str, pd.DataFrame]:
    """
    Prepara os dados para análise de orçamento vs pedido.
    
    Args:
        dataframes: Dicionário contendo os DataFrames carregados
        
    Returns:
        Dicionário contendo os DataFrames processados
    """

    # Verificar se os dataframes necessários estão presentes
    required_dfs = ['vendas', 'clientes', 'venda_itens']
    if not all(df in dataframes for df in required_dfs):
        print("Erro: Dados necessários não estão disponíveis")
        return {}
    
    # Obter os dataframes
    df_vendas = dataframes['vendas']
    df_clientes = dataframes['clientes']
    df_venda_itens = dataframes['venda_itens']
    
    # Converter data_venda para datetime e criar coluna Ano
    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
    df_vendas['ano'] = df_vendas['data_venda'].dt.year
    
    # Preparar os dados de venda_itens
    df_venda_itens['tipo'] = df_venda_itens['tipo'].fillna('N/A')  # Tratando possíveis valores nulos
    
    # Mesclar df_vendas com df_venda_itens para obter as informações de tipo
    df_venda_itens_com_data = df_venda_itens.merge(
        df_vendas[['id_venda', 'data_venda']], 
        on='id_venda', 
        how='left'
    )
    
    # Converter data_venda para datetime e criar coluna Ano para venda_itens
    df_venda_itens_com_data['data_venda'] = pd.to_datetime(df_venda_itens_com_data['data_venda'])
    df_venda_itens_com_data['ano'] = df_venda_itens_com_data['data_venda'].dt.year
    
    # Certifique-se de que total_item seja numérico
    df_venda_itens_com_data['total_item'] = pd.to_numeric(df_venda_itens_com_data['total_item'], errors='coerce')
    
    # Definir o período de 6 meses a partir da data atual
    data_atual = datetime.now()
    data_limite = data_atual - pd.DateOffset(months=6)
    
    # Filtrar vendas nos últimos 6 meses
    df_vendas_6meses = df_vendas[(df_vendas['data_venda'] >= data_limite) & (df_vendas['data_venda'] <= data_atual)]
    
    # Obter IDs de clientes ativos nos últimos 6 meses
    clientes_ativos = df_vendas_6meses['id_cliente'].unique()
    
    # Filtrar DataFrame de clientes para apenas os ativos
    df_clientes_ativos = df_clientes[df_clientes['id_cliente'].isin(clientes_ativos)]
    
    # Mesclar vendas de 6 meses com os itens de venda para obter valores totais
    df_vendas_itens_6meses = df_venda_itens_com_data.merge(
        df_vendas_6meses[['id_venda', 'id_cliente', 'tipo_venda', 'situacao_venda']], 
        on='id_venda', 
        how='inner'
    )
    
    # Retornar DataFrames processados
    return {
        'vendas': df_vendas,
        'clientes': df_clientes,
        'venda_itens': df_venda_itens,
        'venda_itens_com_data': df_venda_itens_com_data,
        'vendas_6meses': df_vendas_6meses,
        'clientes_ativos': df_clientes_ativos,
        'vendas_itens_6meses': df_vendas_itens_6meses
    }
        
def analisar_orcamento_pedido(
    dados_processados: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Analisa a relação entre orçamentos e pedidos concluídos.
    
    Args:
        dados_processados: Dicionário contendo os DataFrames processados
        
    Returns:
        DataFrame com a análise de orçamentos vs pedidos
    """
    # Verificar se os dataframes necessários estão presentes
    required_dfs = ['clientes_ativos', 'vendas_itens_6meses']
    if not all(df in dados_processados for df in required_dfs):
        print("Erro: Dados processados necessários não estão disponíveis")
        return pd.DataFrame()
    
    # Obter os dataframes
    df_clientes_ativos = dados_processados['clientes_ativos']
    df_vendas_itens_6meses = dados_processados['vendas_itens_6meses']
    
    # Agrupar por cliente e calcular o valor total orçado
    df_orcamentos = df_vendas_itens_6meses[df_vendas_itens_6meses['tipo_venda'] == 'ORCAMENTO']
    df_valor_orcado = df_orcamentos.groupby('id_cliente')['total_item'].sum().reset_index()
    df_valor_orcado.rename(columns={'total_item': 'valor_orcado_6meses'}, inplace=True)
    
    # Agrupar por cliente e calcular o valor total de pedidos concluídos
    df_pedidos_concluidos = df_vendas_itens_6meses[
        (df_vendas_itens_6meses['tipo_venda'] == 'PEDIDO') & 
        (df_vendas_itens_6meses['situacao_venda'] == 'CONCLUIDA')
    ]
    df_valor_pedido = df_pedidos_concluidos.groupby('id_cliente')['total_item'].sum().reset_index()
    df_valor_pedido.rename(columns={'total_item': 'valor_pedido_concluido_6meses'}, inplace=True)
    
    # Criar DataFrame base com todos os clientes ativos
    df_clientes_orcamento_pedido = df_clientes_ativos[['id_cliente', 'nome']].copy()
    
    # Adicionar colunas de valor orçado e valor de pedido concluído
    df_clientes_orcamento_pedido = df_clientes_orcamento_pedido.merge(df_valor_orcado, on='id_cliente', how='left')
    df_clientes_orcamento_pedido = df_clientes_orcamento_pedido.merge(df_valor_pedido, on='id_cliente', how='left')
    
    # Preencher valores NaN com 0
    df_clientes_orcamento_pedido['valor_orcado_6meses'] = df_clientes_orcamento_pedido['valor_orcado_6meses'].fillna(0)
    df_clientes_orcamento_pedido['valor_pedido_concluido_6meses'] = df_clientes_orcamento_pedido['valor_pedido_concluido_6meses'].fillna(0)
    
    return df_clientes_orcamento_pedido
def analisar_top_categorias(
    dados_processados: Dict[str, pd.DataFrame],
    dataframes: Dict[str, Union[pd.DataFrame, Dict[str, str]]],
    tipo_analise: str
) -> pd.DataFrame:
    """
    Analisa as top 3 categorias por cliente para orçamentos ou pedidos.
    
    Args:
        dados_processados: Dicionário contendo os DataFrames processados
        dataframes: Dicionário contendo os DataFrames originais
        tipo_analise: 'ORCAMENTO' ou 'PEDIDO' para indicar o tipo de análise
        
    Returns:
        DataFrame com as top 3 categorias por cliente
    """
    # Verificar se os dados necessários estão presentes
    if 'vendas_itens_6meses' not in dados_processados or 'produtos' not in dataframes or 'categorias' not in dataframes:
        print("Erro: Dados necessários para análise de categorias não estão disponíveis")
        return pd.DataFrame()
    
    # Obter DataFrames necessários
    df_vendas_itens_6meses = dados_processados['vendas_itens_6meses']
    df_clientes_ativos = dados_processados['clientes_ativos']
    df_produtos = dataframes['produtos']
    df_categorias = dataframes['categorias']
    
    # Mesclar produtos com os itens de venda para obter as categorias
    df_vendas_itens_com_categoria = df_vendas_itens_6meses.merge(
        df_produtos, 
        on='id_produto', 
        how='left'
    )
    
    # Mesclar com df_categorias para obter os nomes das categorias
    df_vendas_itens_completo = df_vendas_itens_com_categoria.merge(
        df_categorias[['id_categoria', 'nome_categoria']], 
        on='id_categoria', 
        how='left'
    )
    
    # Substituir valores nulos na categoria
    df_vendas_itens_completo['nome_categoria'] = df_vendas_itens_completo['nome_categoria'].fillna('Sem Categoria')
    
    # Filtragem de acordo com tipo_analise
    if tipo_analise == 'ORCAMENTO':
        df_filtrado = df_vendas_itens_completo[df_vendas_itens_completo['tipo_venda'] == 'ORCAMENTO']
    elif tipo_analise == 'PEDIDO':
        df_filtrado = df_vendas_itens_completo[
            (df_vendas_itens_completo['tipo_venda'] == 'PEDIDO') &
            (df_vendas_itens_completo['situacao_venda'] == 'CONCLUIDA')
        ]
    else:
        print(f"Tipo de análise inválido: {tipo_analise}. Use 'ORCAMENTO' ou 'PEDIDO'.")
        return pd.DataFrame()
    
    # Para cada cliente, calcular o valor total por categoria
    df_categoria_cliente = df_filtrado.groupby(['id_cliente', 'nome_categoria'])['total_item'].sum().reset_index()
    
    # Calcular o valor total por cliente para calcular percentuais
    df_total_cliente = df_filtrado.groupby('id_cliente')['total_item'].sum().reset_index()
    df_total_cliente.rename(columns={'total_item': f'total_{tipo_analise.lower()}_cliente'}, inplace=True)
    
    # Mesclar os totais com os valores por categoria
    df_categoria_cliente = df_categoria_cliente.merge(
        df_total_cliente, 
        on='id_cliente',
        how='left'
    )
    
    # Calcular percentual por categoria
    df_categoria_cliente['percentual'] = (
        df_categoria_cliente['total_item'] / df_categoria_cliente[f'total_{tipo_analise.lower()}_cliente']
    ) * 100
    
    # Para cada cliente, obter as top 3 categorias
    def get_top3_categorias(grupo):
        return grupo.nlargest(3, 'total_item')
    
    df_top3_categorias = df_categoria_cliente.groupby('id_cliente').apply(get_top3_categorias).reset_index(drop=True)
    
    # Mesclar com informações do cliente
    df_top3_categorias_final = df_top3_categorias.merge(
        df_clientes_ativos[['id_cliente', 'nome']], 
        on='id_cliente',
        how='left'
    )
    
    # Formatar os valores e ordenar
    df_top3_categorias_final = df_top3_categorias_final[['nome', 'id_cliente', 'nome_categoria', 'total_item', 'percentual']]
    df_top3_categorias_final['total_item'] = df_top3_categorias_final['total_item'].round(2)
    df_top3_categorias_final['percentual'] = df_top3_categorias_final['percentual'].round(2)
    df_top3_categorias_final = df_top3_categorias_final.sort_values(['nome', 'total_item'], ascending=[True, False])
    
    return df_top3_categorias_final


def _agrupar_categorias_por_cliente(
    df_top3_categorias: pd.DataFrame
) -> Dict[int, Dict[str, List]]:
    """
    Agrupa as categorias por cliente em um formato adequado para o relatório.
    
    Args:
        df_top3_categorias: DataFrame com as top 3 categorias
        
    Returns:
        Dicionário com as categorias agrupadas por cliente
    """
    top_categorias = {}
    
    for cliente_id in df_top3_categorias['id_cliente'].unique():
        categorias = df_top3_categorias[df_top3_categorias['id_cliente'] == cliente_id]
        categorias_nomes = []
        categorias_valores = []
        categorias_percentuais = []
        
        for _, row in categorias.iterrows():
            categorias_nomes.append(row['nome_categoria'])
            categorias_valores.append(row['total_item'])
            categorias_percentuais.append(row['percentual'])
        
        # Preencher com 'N/A' se não tiver 3 categorias
        while len(categorias_nomes) < 3:
            categorias_nomes.append("N/A")
            categorias_valores.append(0)
            categorias_percentuais.append(0)
            
        top_categorias[cliente_id] = {
            'nomes': categorias_nomes,
            'valores': categorias_valores,
            'percentuais': categorias_percentuais
        }
        
    return top_categorias


def gerar_relatorio_consolidado(
    df_clientes_orcamento_pedido: pd.DataFrame,
    df_top3_categorias_orcamento: pd.DataFrame,
    df_top3_categorias_pedido: pd.DataFrame
) -> pd.DataFrame:
    """
    Gera um relatório consolidado com orçamentos, pedidos e top categorias.
    
    Args:
        df_clientes_orcamento_pedido: DataFrame com análise de orçamentos vs pedidos
        df_top3_categorias_orcamento: DataFrame com top 3 categorias de orçamentos
        df_top3_categorias_pedido: DataFrame com top 3 categorias de pedidos
        
    Returns:
        DataFrame consolidado
    """
    # Criar dicionários para armazenar as top categorias por cliente
    top_categorias_orcamento = _agrupar_categorias_por_cliente(df_top3_categorias_orcamento)
    top_categorias_pedido = _agrupar_categorias_por_cliente(df_top3_categorias_pedido)
    
    # Criar DataFrame consolidado
    consolidado_data = []
    
    for _, row in df_clientes_orcamento_pedido.iterrows():
        cliente_id = row['id_cliente']
        nome_cliente = row['nome']
        valor_orcado = row['valor_orcado_6meses']
        valor_pedido = row['valor_pedido_concluido_6meses']
        
        # Obter top categorias de orçamento (ou valores vazios se não tiver)
        default_vazio = {'nomes': ["N/A", "N/A", "N/A"], 'valores': [0, 0, 0], 'percentuais': [0, 0, 0]}
        cat_orcamento = top_categorias_orcamento.get(cliente_id, default_vazio)
        
        # Obter top categorias de pedido (ou valores vazios se não tiver)
        cat_pedido = top_categorias_pedido.get(cliente_id, default_vazio)
        
        # Calcular taxa de conversão (evitando divisão por zero)
        taxa_conversao = round((1-(valor_orcado/(valor_pedido+valor_orcado))) * 100, 2) if valor_orcado > 0 else 0
        
        # Adicionar linha ao DataFrame consolidado
        consolidado_data.append({
            'id_cliente': cliente_id,
            'nome_cliente': nome_cliente,
            'valor_orcado_6meses': round(valor_orcado, 2),
            'valor_pedido_concluido_6meses': round(valor_pedido, 2),
            'taxa_conversao': taxa_conversao,
            # Categorias de orçamento separadas por nome, valor e percentual
            'top1_categoria_orcamento_nome': cat_orcamento['nomes'][0],
            'top1_categoria_orcamento_valor': round(cat_orcamento['valores'][0], 2),
            'top1_categoria_orcamento_taxa': round(cat_orcamento['percentuais'][0], 1),
            'top2_categoria_orcamento_nome': cat_orcamento['nomes'][1],
            'top2_categoria_orcamento_valor': round(cat_orcamento['valores'][1], 2),
            'top2_categoria_orcamento_taxa': round(cat_orcamento['percentuais'][1], 1),
            'top3_categoria_orcamento_nome': cat_orcamento['nomes'][2],
            'top3_categoria_orcamento_valor': round(cat_orcamento['valores'][2], 2),
            'top3_categoria_orcamento_taxa': round(cat_orcamento['percentuais'][2], 1),
            # Categorias de pedido separadas por nome, valor e percentual
            'top1_categoria_pedido_nome': cat_pedido['nomes'][0],
            'top1_categoria_pedido_valor': round(cat_pedido['valores'][0], 2),
            'top1_categoria_pedido_taxa': round(cat_pedido['percentuais'][0], 1),
            'top2_categoria_pedido_nome': cat_pedido['nomes'][1],
            'top2_categoria_pedido_valor': round(cat_pedido['valores'][1], 2),
            'top2_categoria_pedido_taxa': round(cat_pedido['percentuais'][1], 1),
            'top3_categoria_pedido_nome': cat_pedido['nomes'][2],
            'top3_categoria_pedido_valor': round(cat_pedido['valores'][2], 2),
            'top3_categoria_pedido_taxa': round(cat_pedido['percentuais'][2], 1)
        })
    
    # Criar DataFrame consolidado
    df_consolidado = pd.DataFrame(consolidado_data)
    
    # Ordenar por valor de orçamento (do maior para o menor)
    df_consolidado = df_consolidado.sort_values('valor_orcado_6meses', ascending=False)

    # Retirar linhas onde valor_orcado_6meses é igual a zero
    df_consolidado = df_consolidado[df_consolidado['valor_orcado_6meses'] > 0]
    
    return df_consolidado


def salvar_relatorio(
    df_consolidado: pd.DataFrame,
    nome_arquivo: str = 'orcamento_x_pedido.csv'
) -> str:
    """
    Salva o relatório consolidado em um arquivo CSV.
    
    Args:
        df_consolidado: DataFrame com o relatório consolidado
        nome_arquivo: Nome do arquivo de saída
        
    Returns:
        Caminho completo para o arquivo salvo
    """
    # Criar diretório para salvar os relatórios
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    diretorio_relatorios = os.path.join(diretorio_atual, 'relatorios')
    os.makedirs(diretorio_relatorios, exist_ok=True)
    
    # Caminho completo para o arquivo
    caminho_arquivo = os.path.join(diretorio_relatorios, nome_arquivo)
    
    # Salvar o DataFrame consolidado em CSV
    df_consolidado.to_csv(caminho_arquivo, index=False, sep=';', encoding='utf-8-sig')
    
    return caminho_arquivo


def gerar_relatorios_orcamento(nome_cliente: str) -> None:
    """
    Gera relatórios de orçamento para o cliente especificado.
    
    Esta função executa todo o fluxo de análise de orçamentos:
    1. Carrega os dados do cliente
    2. Prepara os dados para análise
    3. Analisa a relação entre orçamentos e pedidos
    4. Analisa as top 3 categorias para orçamentos e pedidos
    5. Gera um relatório consolidado
    6. Salva o relatório em um arquivo CSV
    
    Args:
        nome_cliente: Nome do cliente para o qual gerar os relatórios
    """
    # Carregar dados do cliente
    dataframes = carregar_dados_cliente(nome_cliente)
    if not dataframes:
        return
    
    try:
        # Preparar dados para análise
        dados_processados = preparar_dados_orcamento_pedido(dataframes)
        if not dados_processados:
            return
        
        # Analisar orçamentos vs pedidos
        df_clientes_orcamento_pedido = analisar_orcamento_pedido(dados_processados)
        num_clientes_ativos = len(df_clientes_orcamento_pedido)
        print(f"\nNúmero de clientes ativos nos últimos 6 meses: {num_clientes_ativos}")
        print("\nRelatório de Clientes Ativos - Orçamentos x Pedidos Concluídos (últimos 6 meses):")
        print(df_clientes_orcamento_pedido.head())
        
        # Analisar top 3 categorias para orçamentos
        print("\nGerando top 3 categorias por cliente para ORÇAMENTOS...")
        df_top3_categorias_orcamento = analisar_top_categorias(dados_processados, dataframes, 'ORCAMENTO')
        print("\nTop 3 categorias por cliente (ORÇAMENTOS):")
        print(df_top3_categorias_orcamento.head(10))
        
        # Analisar top 3 categorias para pedidos
        print("\nGerando top 3 categorias por cliente para PEDIDOS...")
        df_top3_categorias_pedido = analisar_top_categorias(dados_processados, dataframes, 'PEDIDO')
        print("\nTop 3 categorias por cliente (PEDIDOS):")
        print(df_top3_categorias_pedido.head(10))
        
        # Gerar relatório consolidado
        print("\nGerando tabela consolidada de clientes com orçamentos, pedidos e top categorias...")
        df_consolidado = gerar_relatorio_consolidado(
            df_clientes_orcamento_pedido,
            df_top3_categorias_orcamento,
            df_top3_categorias_pedido
        )
        
        # Salvar relatório
        caminho_arquivo = salvar_relatorio(df_consolidado)
        print(f"\nRelatório consolidado salvo em: {caminho_arquivo}")
        print("\nPrimeiras linhas do relatório consolidado:")
        print(df_consolidado.head())
        
    except Exception as e:
        print(f"Erro ao gerar relatórios: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    # Exemplo de execução para o cliente "add"
    gerar_relatorios_orcamento("add")
