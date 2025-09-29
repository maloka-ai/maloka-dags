from datetime import datetime, timedelta
import pandas as pd
import os
import warnings
import psycopg2
import numpy as np
import argparse
import sys
from multiprocessing import Pool, cpu_count
import tempfile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import get_db_config_maloka_instance

# Obtém a configuração inicial do banco
DB_CONFIG_MALOKA = get_db_config_maloka_instance()

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Função auxiliar para processar lotes em paralelo
def processar_lote(args):
    """
    Função para processar lotes de dados em paralelo.
    Recebe uma tupla com os argumentos necessários para fazer a inserção.
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
        
        return len(lote_dados), None  # Retorna quantidade de registros inseridos e None para erro
    except Exception as e:
        return 0, str(e)  # Retorna 0 registros inseridos e a mensagem de erro

def gerar_relatorios_compra(nome_cliente):
    """
    Gera relatórios de compras atípicas para um cliente específico.
    """
    # Verificar se o cliente existe na configuração
    if nome_cliente not in CLIENTES:
        print(f"Erro: Cliente '{nome_cliente}' não encontrado na configuração!")
        print(f"Clientes disponíveis: {', '.join(CLIENTES.keys())}")
        return
    
    # Carregar configurações do cliente
    config_cliente = CLIENTES[nome_cliente]
    database = config_cliente["database"]
    schema = config_cliente["schema"]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    
    # Criar diretório para salvar os relatórios do cliente
    # diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_compras', nome_cliente)
    # os.makedirs(diretorio_cliente, exist_ok=True)
    
    print(f"Gerando relatórios para o cliente: {nome_cliente}")
    print(f"Database: {database}, Schema: {schema}")
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
        
        ########################################################
        # consulta da tabela vendas
        ########################################################
        
        print("Consultando a tabela VENDAS...")
        query = f"""
        SELECT id_venda, id_cliente, data_venda, total_venda, situacao_venda, tipo_venda
        FROM {schema}.venda
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_vendas = pd.read_sql_query(query, conn)
        
        # Converter id_venda para longint (int64)
        if 'id_venda' in df_vendas.columns:
            df_vendas['id_venda'] = df_vendas['id_venda'].astype('int64')
        
        # Informações sobre os dados
        num_registros = len(df_vendas)
        num_colunas = len(df_vendas.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_vendas.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_vendas.head())
        
        # Exportar para Excel
        # df_vendas.to_excel("df_vendas_BD.xlsx", index=False)

        ########################################################
        # consulta da tabela venda_itens
        ########################################################
        
        print("Consultando a tabela VENDA_ITEM...")
        query = f"""
        SELECT id_venda_item, id_venda, id_produto, quantidade, total_item
        FROM {schema}.venda_item
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_venda_itens = pd.read_sql_query(query, conn)
        
        # Converter id_venda para longint (int64)
        if 'id_venda' in df_venda_itens.columns:
            df_venda_itens['id_venda'] = df_venda_itens['id_venda'].astype('int64')
        
        # Informações sobre os dados
        num_registros = len(df_venda_itens)
        num_colunas = len(df_venda_itens.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_venda_itens.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_venda_itens.head())
        
        # Exportar para Excel
        # df_venda_itens.to_excel("df_venda_itens_BD.xlsx", index=False)

        ########################################################
        # consulta da tabela produto
        ########################################################
        
        print("Consultando a tabela PRODUTO...")
        query = f"""
        SELECT id_produto, nome AS nome_produto, id_categoria, prazo_reposicao_dias
        FROM {schema}.produto
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_produto = pd.read_sql_query(query, conn)
        
        # Converter id_produto para inteiro para garantir que não tenha .0
        if 'id_produto' in df_produto.columns:
            df_produto['id_produto'] = df_produto['id_produto'].astype(int)
        
        # Informações sobre os dados
        num_registros = len(df_produto)
        num_colunas = len(df_produto.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_produto.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_produto.head())
        
        # Exportar para Excel
        # df_produto.to_excel("df_produto_BD.xlsx", index=False)

        ########################################################
        # consulta da tabela compra
        ########################################################
        
        print("Consultando a tabela COMPRA...")
        query = f"""
        SELECT id_compra, id_fornecedor, data_compra, total_compra
        FROM {schema}.compra
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_compra = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_compra)
        num_colunas = len(df_compra.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_compra.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_compra.head())
        
        # Exportar para Excel
        # df_compra.to_excel("df_compra_BD.xlsx", index=False)

        ########################################################
        # consulta da tabela compra
        ########################################################
        
        print("Consultando a tabela COMPRA_ITEM...")
        query = f"SELECT * FROM {schema}.compra_item"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_compra_item = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_compra_item)
        num_colunas = len(df_compra_item.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_compra_item.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_compra_item.head())
        
        # Exportar para Excel
        # df_compra_item.to_excel("df_compra_item_BD.xlsx", index=False)

        ########################################################
        # consulta da tabela fornecedor
        ########################################################
        
        print("Consultando a tabela FORNECEDOR...")
        query = f"""
        SELECT id_fornecedor, cpf_cnpj, nome AS nome_fornecedor
        FROM {schema}.fornecedor
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_fornecedor = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_fornecedor)
        num_colunas = len(df_fornecedor.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_fornecedor.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_fornecedor.head())
        
        # Exportar para Excel
        # df_fornecedor.to_excel("df_fornecedor_BD.xlsx", index=False)

        ########################################################
        # consulta da tabela estoque_movimento
        ########################################################
        
        print("Consultando a tabela ESTOQUE_MOVIMENTO...")
        query = f"""
        SELECT id_estoque_movimento, id_produto, quantidade, data_movimento, tipo, estoque_depois
        FROM {schema}.estoque_movimento
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_estoque_movimento = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_estoque_movimento)
        num_colunas = len(df_estoque_movimento.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_estoque_movimento.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_estoque_movimento.head())
        
        # Exportar para Excel
        # df_estoque_movimento.to_excel("df_estoque_movimento_BD.xlsx", index=False)

        print("Consultando a tabela CATEGORIA...")
        query = f"""
        SELECT id_categoria, nome_categoria
        FROM {schema}.categoria
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_categoria = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_categoria)
        num_colunas = len(df_categoria.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_categoria.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_categoria.head())
        
        # Exportar para Excel
        # df_categoria.to_excel("df_categoria.xlsx", index=False)

        ########################################################
        # consulta da tabela historico_estoque
        ########################################################

        print("Consultando a tabela HISTORICO_ESTOQUE...")
        query = f"""
        SELECT id_produto, preco_custo, data_estoque, id_fornecedor_padrao
        FROM {schema}.historico_estoque
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_historico_estoque = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_historico_estoque)
        num_colunas = len(df_historico_estoque.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_historico_estoque.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_historico_estoque.head())
        
        # Exportar para CSV
        # df_historico_estoque.to_csv("df_historico_estoque.csv", index=False)

        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")

    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")

    print("\n# Iniciando análise para recomendação de compras...")

    # Processando os dados para análise
    try:
        # Converter data_venda para datetime
        df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])

        #filtrar df_vendas para somente vendas que possuem a tipo_venda como PEDIDO e situacao_venda como CONCLUIDA
        df_vendas = df_vendas[(df_vendas['tipo_venda'] == 'PEDIDO') & (df_vendas['situacao_venda'] == 'CONCLUIDA')]
        
        # Mesclar vendas e itens de venda
        df_vendas_completo = df_vendas.merge(
            df_venda_itens, 
            on='id_venda', 
            how='left'
        )
        
        # Adicionar informações do produto
        df_vendas_completo = df_vendas_completo.merge(
            df_produto[['id_produto', 'nome_produto', 'id_categoria']],
            on='id_produto',
            how='left'
        )

        #Pegar o preço de custo mais recente para cada produto
        df_historico_estoque_agrupado = df_historico_estoque.sort_values('data_estoque', ascending=False).drop_duplicates('id_produto')
        df_vendas_completo = df_vendas_completo.merge(
            df_historico_estoque_agrupado[['id_produto', 'preco_custo', 'id_fornecedor_padrao']],
            on='id_produto',
            how='left'
        )
        
        print(f"Dados de vendas processados: {len(df_vendas_completo)} registros")
        
        # Calcular data atual e período de análise
        data_atual = datetime.now()
        periodo_12_meses = data_atual - timedelta(days=364)
        
        # Filtrar vendas dos últimos 12 meses
        df_vendas_1ano = df_vendas_completo[df_vendas_completo['data_venda'] >= periodo_12_meses].copy()
        
        print(f"Vendas no último ano: {len(df_vendas_1ano)} registros")
        
        #Calcular diretamente com dados do último ano (12 meses)
        vendas_mensais_12m = df_vendas_1ano.groupby('id_produto').agg(
            quantidade_total_12m=('quantidade', 'sum'),
            valor_total_12m=('total_item', 'sum'),
            transacoes_12m=('id_venda', 'nunique')
        ).reset_index()
        
        # Calcular média mensal (dividindo por 12 meses)
        vendas_mensais_12m['media_12m_qtd'] = vendas_mensais_12m['quantidade_total_12m'] / 12
        vendas_mensais_12m['media_mensal_valor'] = vendas_mensais_12m['valor_total_12m'] / 12

        # Adicionar análise de consumo mês a mês no último ano
        print("Calculando consumo mês a mês no último ano...")
        
        # Extrair mês e ano de cada venda
        df_vendas_1ano['mes_ano'] = df_vendas_1ano['data_venda'].dt.strftime('%Y-%m')
        # df_vendas_1ano.to_excel("df_vendas_1ano.xlsx", index=False)

        # Criar tabela pivô com o consumo mensal por produto
        consumo_mensal = df_vendas_1ano.pivot_table(
            index='id_produto',
            columns='mes_ano',
            values='quantidade',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Obter lista de todos os meses no período analisado (em ordem cronológica)
        meses_ordenados = sorted(df_vendas_1ano['mes_ano'].unique())

        # print("\nMeses incluídos na análise de consumo:")
        # print(', '.join(meses_ordenados))
        
        # Dicionário para mapear os nomes das colunas
        colunas_renomeadas = {}
        
        # Se tivermos meses suficientes no histórico
        if meses_ordenados:
            # O último mês na lista ordenada é o mais recente
            for i, mes in enumerate(reversed(meses_ordenados)):
                if i == 0:
                    # Mês mais recente (atual)
                    colunas_renomeadas[mes] = 'qtd_vendas_mes_atual'
                elif i < 12:  # Limitar a 12 meses no máximo
                    # Meses anteriores: 1m_atras, 2m_atras, etc.
                    colunas_renomeadas[mes] = f'qtd_vendas_{i}m_atras'
        
        # Aplicar renomeação
        consumo_mensal = consumo_mensal.rename(columns=colunas_renomeadas)
        
        # print(f"Consumo mensal calculado para {len(consumo_mensal)} produtos com nomenclatura padronizada")
        # print(f"Colunas padronizadas: {', '.join([col for col in consumo_mensal.columns if col.startswith('qtd_vendas_')])}")
        # Calcular a média de vendas mensais por produto, mas apenas para produtos vendidos no último ano
        # produtos_vendidos_ultimo_ano = df_vendas_1ano['id_produto'].unique()

        # Obter estoque atual por produto
        df_estoque_atual = df_estoque_movimento.sort_values('data_movimento', ascending=False)
        df_estoque_atual = df_estoque_atual.drop_duplicates(subset=['id_produto'])
        df_estoque_atual = df_estoque_atual[['id_produto', 'estoque_depois', 'data_movimento']]
        df_estoque_atual = df_estoque_atual.rename(columns={'estoque_depois': 'estoque_atual', 
                                                        'data_movimento': 'data_ultima_movimentacao'})
        
        # Combinar médias de vendas com estoque atual
        df_recomendacao = vendas_mensais_12m.merge(
            df_estoque_atual,
            on='id_produto',
            how='left'
        )
        
        # Adicionar informações do produto
        df_recomendacao = df_recomendacao.merge(
            df_produto[['id_produto', 'nome_produto']],
            on='id_produto',
            how='left'
        )

        df_historico_estoque_agrupado = df_historico_estoque.sort_values('data_estoque', ascending=False).drop_duplicates('id_produto')
        df_recomendacao = df_recomendacao.merge(
            df_historico_estoque_agrupado[['id_produto', 'preco_custo', 'id_fornecedor_padrao']],
            on='id_produto',
            how='left'
        )
        
        # Calcular métricas de recomendação
        df_recomendacao['estoque_atual'] = df_recomendacao['estoque_atual'].fillna(0)
        df_recomendacao['cobertura_meses'] = df_recomendacao.apply(
            lambda x: 0 if x['estoque_atual'] <= 0 else 
                    (x['estoque_atual'] / x['media_12m_qtd'] if x['media_12m_qtd'] > 0 else float('inf')),
            axis=1
        )

        # Adicionar cobertura em dias (1 mês = 30 dias)
        df_recomendacao['cobertura_dias'] = df_recomendacao['cobertura_meses'] * 30
        
        # Adicionar cobertura em percentual (considerando 1 mês como 100%)
        df_recomendacao['cobertura_percentual_30d'] = df_recomendacao['cobertura_meses'] * 100

        # Calcular sugestão de compra para 1 mês de estoque
        df_recomendacao['sugestao_1m'] = df_recomendacao.apply(
            lambda x: 0 if x['media_12m_qtd'] <= 0 else (
                # Para estoque negativo: 1 mês de estoque + compensação do negativo
                (1 * x['media_12m_qtd'] - x['estoque_atual']) if x['estoque_atual'] < 0 
                # Para estoque positivo: complemento até 1 mês, se necessário
                else max(0, 1 * x['media_12m_qtd'] - x['estoque_atual'])
            ),
            axis=1
        )
        # Arredondar sugestão para cima (não queremos frações de produtos)
        df_recomendacao['sugestao_1m'] = np.ceil(df_recomendacao['sugestao_1m'])
        
        # Calcular sugestão de compra para 3 meses de estoque
        df_recomendacao['sugestao_3m'] = df_recomendacao.apply(
            lambda x: 0 if x['media_12m_qtd'] <= 0 else (
                # Para estoque negativo: 3 meses de estoque + compensação do negativo
                (3 * x['media_12m_qtd'] - x['estoque_atual']) if x['estoque_atual'] < 0 
                # Para estoque positivo: complemento até 3 meses, se necessário
                else max(0, 3 * x['media_12m_qtd'] - x['estoque_atual'])
            ),
            axis=1
        )
        # Arredondar sugestão para cima (não queremos frações de produtos)
        df_recomendacao['sugestao_3m'] = np.ceil(df_recomendacao['sugestao_3m'])

        # Calcular sugestão para de compra para 6 meses de estoque
        df_recomendacao['sugestao_6m'] = df_recomendacao.apply(
            lambda x: 0 if x['media_12m_qtd'] <= 0 else (
                # Para estoque negativo: 6 meses de estoque + compensação do negativo
                (6 * x['media_12m_qtd'] - x['estoque_atual']) if x['estoque_atual'] < 0 
                # Para estoque positivo: complemento até 6 meses, se necessário
                else max(0, 6 * x['media_12m_qtd'] - x['estoque_atual'])
            ),
            axis=1
        )
        # Arredondar sugestão para cima (não queremos frações de produtos)
        df_recomendacao['sugestao_6m'] = np.ceil(df_recomendacao['sugestao_6m'])

        # Classificar criticidade do estoque
        def classificar_criticidade(cobertura):
            if cobertura < 0.3:  # Menos de 30%
                return "CRÍTICO"
            elif cobertura < 0.5:  # Entre 30% e 50%
                return "MUITO BAIXO"
            elif cobertura < 0.8:  # Entre 50% e 80%
                return "BAIXO"
            elif cobertura <= 1.0:  # Entre 80% e 100%
                return "ADEQUADO"
            else:  # Acima de 100%
                return "EXCESSO"
        
        df_recomendacao['criticidade'] = df_recomendacao['cobertura_meses'].apply(classificar_criticidade)

        # Obter as últimas 3 compras para cada produto (última, penúltima e antepenúltima)
        print("Processando histórico de compras para obter últimas 3 aquisições...")
        df_historico_compras = df_compra_item.merge(df_compra, on='id_compra', how='left')
        df_historico_compras = df_historico_compras.sort_values(['id_produto', 'data_compra'], ascending=[True, False])

        # Função para extrair as 3 compras mais recentes por produto
        ultimas_compras_dict = {}
        penultimas_compras_dict = {}
        antepenultimas_compras_dict = {}

        for produto_id in df_historico_compras['id_produto'].unique():
            # Filtrar compras deste produto e ordená-las por data (mais recente primeiro)
            compras_produto = df_historico_compras[df_historico_compras['id_produto'] == produto_id]
            compras_produto = compras_produto.sort_values('data_compra', ascending=False)
            
            # Adicionar última compra
            if len(compras_produto) >= 1:
                ultima = compras_produto.iloc[0]
                ultimas_compras_dict[produto_id] = {
                    'id_fornecedor': int(ultima['id_fornecedor']) if pd.notna(ultima['id_fornecedor']) else None,
                    'preco_bruto': ultima['preco_bruto'],
                    'data_compra': ultima['data_compra'],
                    'quantidade': ultima['quantidade']
                }
            
            # Adicionar penúltima compra
            if len(compras_produto) >= 2:
                penultima = compras_produto.iloc[1]
                penultimas_compras_dict[produto_id] = {
                    'id_fornecedor': int(penultima['id_fornecedor']) if pd.notna(penultima['id_fornecedor']) else None,
                    'preco_bruto': penultima['preco_bruto'],
                    'data_compra': penultima['data_compra'],
                    'quantidade': penultima['quantidade']
                }
            
            # Adicionar antepenúltima compra
            if len(compras_produto) >= 3:
                antepenultima = compras_produto.iloc[2]
                antepenultimas_compras_dict[produto_id] = {
                    'id_fornecedor': int(antepenultima['id_fornecedor']) if pd.notna(antepenultima['id_fornecedor']) else None,
                    'preco_bruto': antepenultima['preco_bruto'],
                    'data_compra': antepenultima['data_compra'],
                    'quantidade': antepenultima['quantidade']
                }

        # Converter dicionários para DataFrames
        df_ultima_compra = pd.DataFrame.from_dict(ultimas_compras_dict, orient='index').reset_index()
        if not df_ultima_compra.empty:
            df_ultima_compra.rename(columns={
                'index': 'id_produto',
                'preco_bruto': 'ultimo_preco_compra',
                'data_compra': 'data_ultima_compra',
                'quantidade': 'ultima_qtd_comprada'
            }, inplace=True)

        df_penultima_compra = pd.DataFrame.from_dict(penultimas_compras_dict, orient='index').reset_index()
        if not df_penultima_compra.empty:
            df_penultima_compra.rename(columns={
                'index': 'id_produto',
                'preco_bruto': 'penultimo_preco_compra',
                'data_compra': 'data_penultima_compra',
                'quantidade': 'penultima_qtd_comprada'
            }, inplace=True)

        df_antepenultima_compra = pd.DataFrame.from_dict(antepenultimas_compras_dict, orient='index').reset_index()
        if not df_antepenultima_compra.empty:
            df_antepenultima_compra.rename(columns={
                'index': 'id_produto',
                'preco_bruto': 'antepenultimo_preco_compra',
                'data_compra': 'data_antepenultima_compra',
                'quantidade': 'antepenultima_qtd_comprada'
            }, inplace=True)

        # Adicionar informações de fornecedor para cada compra
        if not df_ultima_compra.empty:
            # # Exportar os DataFrames antes das conversões para comparação
            # diretorio_export = os.path.join(diretorio_atual, 'debug_exports')
            # os.makedirs(diretorio_export, exist_ok=True)
            
            # # Exportar DataFrame original de fornecedores
            print("\nExportando DataFrames para comparação...")
            # df_fornecedor.to_csv(os.path.join(diretorio_export, f'{nome_cliente}_df_fornecedor_original.csv'), index=False)
            # df_ultima_compra.to_csv(os.path.join(diretorio_export, f'{nome_cliente}_df_ultima_compra_original.csv'), index=False)
            
            # Antes de qualquer operação, garantir que os id_fornecedor são numéricos
            df_fornecedor['id_fornecedor'] = pd.to_numeric(df_fornecedor['id_fornecedor'], errors='coerce')
            
            # A coluna já deve estar numérica no df_ultima_compra, já que convertemos para int
            # no momento de criar os dicionários, mas vamos garantir isso:
            df_ultima_compra['id_fornecedor'] = pd.to_numeric(df_ultima_compra['id_fornecedor'], errors='coerce').astype('Int64')
            
            # Exportar DataFrames após conversão para verificar os tipos
            # df_fornecedor.to_csv(os.path.join(diretorio_export, f'{nome_cliente}_df_fornecedor_convertido.csv'), index=False)
            # df_ultima_compra.to_csv(os.path.join(diretorio_export, f'{nome_cliente}_df_ultima_compra_convertido.csv'), index=False)
            
            # Agora podemos fazer o merge diretamente sem colunas auxiliares
            print(f"Tipos antes do merge: fornecedor={df_fornecedor['id_fornecedor'].dtype}, ultima_compra={df_ultima_compra['id_fornecedor'].dtype}")
            
            df_ultima_compra = df_ultima_compra.merge(
                df_fornecedor[['id_fornecedor', 'nome_fornecedor']],
                on='id_fornecedor',
                how='left'
            )
            
            print(f"Resultado do merge (ultimos fornecedores): \n{df_ultima_compra['nome_fornecedor'].head()}")
            
            df_ultima_compra.rename(columns={'nome_fornecedor': 'ultimo_fornecedor'}, inplace=True)

        # Mesmo tratamento para penúltima compra
        if not df_penultima_compra.empty:
            # Garantir que id_fornecedor é numérico
            df_penultima_compra['id_fornecedor'] = pd.to_numeric(df_penultima_compra['id_fornecedor'], errors='coerce')
            
            # Fazer o merge diretamente
            df_penultima_compra = df_penultima_compra.merge(
                df_fornecedor[['id_fornecedor', 'nome_fornecedor']],
                on='id_fornecedor',
                how='left'
            )
            
            df_penultima_compra.rename(columns={'nome_fornecedor': 'penultimo_fornecedor'}, inplace=True)

        # Mesmo tratamento para antepenúltima compra
        if not df_antepenultima_compra.empty:
            # Garantir que id_fornecedor é numérico
            df_antepenultima_compra['id_fornecedor'] = pd.to_numeric(df_antepenultima_compra['id_fornecedor'], errors='coerce')
            
            # Fazer o merge diretamente
            df_antepenultima_compra = df_antepenultima_compra.merge(
                df_fornecedor[['id_fornecedor', 'nome_fornecedor']],
                on='id_fornecedor',
                how='left'
            )
    
            df_antepenultima_compra.rename(columns={'nome_fornecedor': 'antepenultimo_fornecedor'}, inplace=True)

        # Adicionar informações das compras ao dataframe de recomendação
        colunas_ultima = ['id_produto', 'ultimo_preco_compra', 'data_ultima_compra', 'ultimo_fornecedor', 'ultima_qtd_comprada', 'id_fornecedor']
        colunas_penultima = ['id_produto', 'penultimo_preco_compra', 'data_penultima_compra', 'penultimo_fornecedor', 'penultima_qtd_comprada']
        colunas_antepenultima = ['id_produto', 'antepenultimo_preco_compra', 'data_antepenultima_compra', 'antepenultimo_fornecedor', 'antepenultima_qtd_comprada']

        if not df_ultima_compra.empty:
            df_recomendacao = df_recomendacao.merge(
                df_ultima_compra[colunas_ultima],
                on='id_produto',
                how='left'
            )

        if not df_penultima_compra.empty:
            df_recomendacao = df_recomendacao.merge(
                df_penultima_compra[colunas_penultima],
                on='id_produto',
                how='left'
            )

        if not df_antepenultima_compra.empty:
            df_recomendacao = df_recomendacao.merge(
                df_antepenultima_compra[colunas_antepenultima],
                on='id_produto',
                how='left'
            )

        # Substitui fornecedor_padrao vazio pelo ID do fornecedor da última compra e garante que seja integer
        print("Verificando fornecedor padrão e substituindo quando necessário...")
        df_recomendacao['id_fornecedor_padrao'] = df_recomendacao.apply(
            lambda x: int(x['id_fornecedor']) if (pd.isna(x['id_fornecedor_padrao']) or x['id_fornecedor_padrao'] == '') and pd.notna(x['id_fornecedor']) else 
                      int(x['id_fornecedor_padrao']) if pd.notna(x['id_fornecedor_padrao']) and x['id_fornecedor_padrao'] != '' else None,
            axis=1
        )
        # Contagem de substituições
        substituicoes = sum(pd.isna(df_recomendacao['id_fornecedor_padrao']) | (df_recomendacao['id_fornecedor_padrao'] == ''))
        print(f"Foram substituídos {substituicoes} fornecedores padrão vazios pelo ID do fornecedor da última compra.")
        
        # Converter a coluna para o tipo integer explicitamente
        df_recomendacao['id_fornecedor_padrao'] = pd.to_numeric(df_recomendacao['id_fornecedor_padrao'], errors='coerce').astype('Int64')
        
        # Adicionar nome do fornecedor padrão
        print("Adicionando nome do fornecedor padrão...")
        df_recomendacao = df_recomendacao.merge(
            df_fornecedor[['id_fornecedor', 'nome_fornecedor']],
            left_on='id_fornecedor_padrao',
            right_on='id_fornecedor',
            how='left'
        )
        # Renomear para evitar conflito com outras colunas de fornecedor
        df_recomendacao.rename(columns={'nome_fornecedor': 'fornecedor_padrao'}, inplace=True)

        # Renomear a coluna id_fornecedor_x para id_ultimo_fornecedor antes de usá-la
        if 'id_fornecedor_x' in df_recomendacao.columns:
            df_recomendacao.rename(columns={'id_fornecedor_x': 'id_ultimo_fornecedor'}, inplace=True)

        # Verificar produtos onde o fornecedor padrão é diferente do fornecedor da última compra
        print("\nVerificando produtos com fornecedor padrão diferente do último fornecedor...")
        produtos_fornecedor_diferente = df_recomendacao[
            # Onde ambos os fornecedores existem
            (pd.notna(df_recomendacao['id_fornecedor_padrao'])) & 
            (pd.notna(df_recomendacao['id_ultimo_fornecedor'])) &
            # E são diferentes
            (df_recomendacao['id_fornecedor_padrao'] != df_recomendacao['id_ultimo_fornecedor'])
        ].copy()
        
        if len(produtos_fornecedor_diferente) > 0:
            print(f"Foram encontrados {len(produtos_fornecedor_diferente)} produtos com fornecedor padrão diferente do fornecedor da última compra.")
            
            # Selecionar colunas relevantes para o relatório
            colunas_relatorio = [
                'id_produto',
                'nome_produto',
                'id_fornecedor_padrao',
                'fornecedor_padrao',
                'id_ultimo_fornecedor',
                'ultimo_fornecedor',
                'data_ultima_compra',
                'estoque_atual',
                'criticidade',
                'media_12m_qtd'
            ]
            
            # Filtrar apenas colunas que existem no DataFrame
            colunas_existentes = [col for col in colunas_relatorio if col in produtos_fornecedor_diferente.columns]
            produtos_fornecedor_diferente = produtos_fornecedor_diferente[colunas_existentes]
            
            ###############################
            # Salvar relatório
            ###############################
            # Exportar para CSV
            # arquivo_relatorio = os.path.join(diretorio_cliente, f'{nome_cliente}_produtos_fornecedor_diferente.csv')
            # produtos_fornecedor_diferente.to_csv(arquivo_relatorio, index=False)
            # print(f"Relatório de produtos com fornecedor diferente exportado para: {arquivo_relatorio}")

            #Quantidade de produtos que possuem fornecedor padrão diferente do último fornecedor
            print(f"Quantidade de produtos que possuem fornecedor padrão diferente do último fornecedor: {len(produtos_fornecedor_diferente)}")
        else:
            print("Todos os produtos possuem o fornecedor padrão igual ao fornecedor da última compra.")
        
        # Remover a coluna id_fornecedor que foi adicionada pelo merge para evitar duplicatas
        if 'id_fornecedor_y' in df_recomendacao.columns:
            df_recomendacao.drop('id_fornecedor_y', axis=1, inplace=True)
        if 'id_fornecedor' in df_recomendacao.columns and 'id_fornecedor_x' not in df_recomendacao.columns:
            df_recomendacao.drop('id_fornecedor', axis=1, inplace=True)

        # Exportar relatório de produtos sem fornecedor (nem padrão, nem da última compra)
        print("\nVerificando produtos sem fornecedor identificado...")
        produtos_sem_fornecedor = df_recomendacao[
            (pd.isna(df_recomendacao['id_fornecedor_padrao']) | (df_recomendacao['id_fornecedor_padrao'] == ''))
        ].copy()

        if len(produtos_sem_fornecedor) > 0:
            print(f"Foram encontrados {len(produtos_sem_fornecedor)} produtos sem fornecedor padrão nem fornecedor da última compra.")
            
            # Selecionar apenas as colunas relevantes para o relatório
            colunas_relatorio = [
                'id_produto', 
                'nome_produto',
                'estoque_atual',
                'data_ultima_compra',
                'ultimo_fornecedor',
                'ultima_qtd_comprada',
                'id_fornecedor_padrao',
                'data_ultima_movimentacao',
                'media_12m_qtd',
                'criticidade'
            ]
            
            # Filtrar apenas colunas que existem no DataFrame
            colunas_existentes = [col for col in colunas_relatorio if col in produtos_sem_fornecedor.columns]
            produtos_sem_fornecedor = produtos_sem_fornecedor[colunas_existentes]
            
            ###############################
            # Salvar relatório
            ###############################
            # Exportar para CSV
            # arquivo_relatorio = os.path.join(diretorio_cliente, f'{nome_cliente}_produtos_sem_fornecedor.csv')
            # produtos_sem_fornecedor.to_csv(arquivo_relatorio, index=False)
            # print(f"Relatório de produtos sem fornecedor exportado para: {arquivo_relatorio}")
        else:
            print("Todos os produtos possuem fornecedor identificado.")

        # Calcular valor estimado da compra sugerida usando o preço da última compra quando disponível
        df_recomendacao['valor_estimado_compra_1m'] = df_recomendacao.apply(
            lambda x: x['sugestao_1m'] * (x['ultimo_preco_compra'] if pd.notna(x['ultimo_preco_compra']) else x['preco_custo']),
            axis=1
        )
        df_recomendacao['valor_estimado_compra_3m'] = df_recomendacao.apply(
            lambda x: x['sugestao_3m'] * (x['ultimo_preco_compra'] if pd.notna(x['ultimo_preco_compra']) else x['preco_custo']),
            axis=1
        )
        df_recomendacao['valor_estimado_compra_6m'] = df_recomendacao.apply(
            lambda x: x['sugestao_6m'] * (x['ultimo_preco_compra'] if pd.notna(x['ultimo_preco_compra']) else x['preco_custo']),
            axis=1
        )
        
        # Adicionar consumo mensal do último ano
        df_recomendacao = df_recomendacao.merge(
            consumo_mensal,
            on='id_produto',
            how='left'
        )
        
        # Preencher valores nulos do consumo mensal com zero
        colunas_consumo = [col for col in df_recomendacao.columns if col.startswith('qtd_vendas_')]
        df_recomendacao[colunas_consumo] = df_recomendacao[colunas_consumo].fillna(0)

        # Calcular média móvel de 3 meses (3M) para cada produto
        print("Calculando média móvel de 3 meses (media_vendas_3m)...")
        
        # Inicializar coluna de média móvel
        df_recomendacao['media_vendas_3m'] = 0

        # Verificar se temos dados suficientes (pelo menos os 3 meses mais recentes)
        colunas_necessarias = ['qtd_vendas_mes_atual', 'qtd_vendas_1m_atras', 'qtd_vendas_2m_atras']
        colunas_disponiveis = [col for col in colunas_necessarias if col in df_recomendacao.columns]

        if len(colunas_disponiveis) >= 3:
            print(f"Calculando média móvel com base nos meses: {', '.join(colunas_disponiveis[:3])}")
            
            # Calcular média dos 3 meses mais recentes
            for produto_idx in df_recomendacao.index:
                soma_3_meses = sum(df_recomendacao.at[produto_idx, col] for col in colunas_disponiveis[:3])
                df_recomendacao.at[produto_idx, 'media_vendas_3m'] = soma_3_meses / 3
        else:
            # Se não tivermos pelo menos 3 meses, usar a média mensal geral
            print("Não há dados suficientes para calcular média móvel de 3 meses. Usando média mensal geral.")
            df_recomendacao['media_vendas_3m'] = df_recomendacao['media_12m_qtd']

        # Arredondar para 2 casas decimais
        df_recomendacao['media_vendas_3m'] = df_recomendacao['media_vendas_3m'].round(2)

        print("Histórico de compras processado com sucesso!")
        
        # Ordenar por criticidade e valor de venda
        ordem_criticidade = {
            "CRÍTICO": 1,
            "MUITO BAIXO": 2,
            "BAIXO": 3,
            "ADEQUADO": 4,
            "EXCESSO": 5
        }

        df_recomendacao['ordem_criticidade'] = df_recomendacao['criticidade'].map(ordem_criticidade)
        df_recomendacao = df_recomendacao.sort_values(['ordem_criticidade'], ascending=[True])
        
        # Obter colunas de consumo mensal
        colunas_consumo = [col for col in df_recomendacao.columns if col.startswith('qtd_vendas_')]

        # Garantir que prazo_reposicao_dias esteja incluído no DataFrame de recomendação
        if 'prazo_reposicao_dias' not in df_recomendacao.columns:
            df_recomendacao = df_recomendacao.merge(
                df_produto[['id_produto', 'prazo_reposicao_dias']],
                on='id_produto',
                how='left'
            )

        # Criar coluna 'critico' baseada no prazo de reposição
        df_recomendacao['critico'] = df_recomendacao['prazo_reposicao_dias'] >= 30

        # Verificar se id_categoria está presente no DataFrame de recomendação
        if 'id_categoria' not in df_recomendacao.columns:
            # Se não estiver, pode-se adicioná-la a partir do DataFrame produto
            df_recomendacao = df_recomendacao.merge(
                df_produto[['id_produto', 'id_categoria']],
                on='id_produto',
                how='left'
            )
            #adicionar o nome da categoria
            df_recomendacao = df_recomendacao.merge(
                df_categoria[['id_categoria', 'nome_categoria']],
                on='id_categoria',
                how='left'
            )
            
        # Selecionar e reorganizar colunas para o relatório final
        colunas_finais = [
            'id_produto', 
            'nome_produto',
            'id_categoria',
            'nome_categoria',
            'estoque_atual', 
            'media_12m_qtd',
            'media_vendas_3m',
            'cobertura_meses', 
            'cobertura_dias',
            'cobertura_percentual_30d',
            'criticidade', 
            'sugestao_1m',
            'valor_estimado_compra_1m',
            'sugestao_3m',
            'valor_estimado_compra_3m',
            'sugestao_6m',
            'valor_estimado_compra_6m',
            'id_fornecedor_padrao',
            'fornecedor_padrao',
            # Última compra
            'ultimo_preco_compra',
            'ultima_qtd_comprada',
            'ultimo_fornecedor', 
            'data_ultima_compra',
            # Penúltima compra
            'penultimo_preco_compra',
            'penultima_qtd_comprada',
            'penultimo_fornecedor',
            'data_penultima_compra',
            # Antepenúltima compra
            'antepenultimo_preco_compra',
            'antepenultima_qtd_comprada',
            'antepenultimo_fornecedor',
            'data_antepenultima_compra',
            # Outras informações
            'data_ultima_movimentacao',
            'transacoes_12m', 
            'quantidade_total_12m', 
            'valor_total_12m',
            'prazo_reposicao_dias',
            'critico'
        ]

        # Adicionar colunas de consumo mensal à lista de colunas finais de forma ordenada
        colunas_consumo_ordenadas = []
        # Primeiro a coluna do mês atual
        if 'qtd_vendas_mes_atual' in colunas_consumo:
            colunas_consumo_ordenadas.append('qtd_vendas_mes_atual')
            
        # Depois os meses anteriores em ordem crescente (1m_atras, 2m_atras, etc.)
        meses_atras = [col for col in colunas_consumo if col != 'qtd_vendas_mes_atual']
        # Extrair o número do mês para ordenar corretamente
        meses_numeros = []
        for col in meses_atras:
            try:
                # Extrair o número do mês da string (por exemplo, 1 de "qtd_vendas_1m_atras")
                num = int(col.split('_')[2].replace('m', ''))
                meses_numeros.append((num, col))
            except (ValueError, IndexError):
                # Se não conseguir extrair o número, apenas adicione a coluna no final
                meses_numeros.append((999, col))
                
        # Ordenar por número do mês
        meses_numeros.sort()
        # Adicionar as colunas ordenadas à lista final
        colunas_consumo_ordenadas.extend([col for _, col in meses_numeros])

        # Adicionar as colunas ordenadas à lista de colunas finais
        colunas_finais.extend(colunas_consumo_ordenadas)
                
        # Manter apenas as colunas que existem
        colunas_existentes = [col for col in colunas_finais if col in df_recomendacao.columns]
        # Criar DataFrame final com as colunas selecionadas
        df_recomendacao_final = df_recomendacao[colunas_existentes].copy()
        
        # Converter id_produto para inteiro para remover o .0
        if 'id_produto' in df_recomendacao_final.columns:
            df_recomendacao_final['id_produto'] = df_recomendacao_final['id_produto'].astype(int)
        
        # Formatar valores decimais
        for col in ['media_12m_qtd', 'media_vendas_3m', 'cobertura_meses', 'cobertura_dias', 'cobertura_percentual_30d', 'valor_estimado_compra_3m', 'valor_estimado_compra_1m']:
            if col in df_recomendacao_final.columns:
                df_recomendacao_final[col] = df_recomendacao_final[col].round(2)
        
        ###############################
        # Salvar relatório
        ###############################
        # Exportar para CSV
        # csv_path = os.path.join(diretorio_cliente, f'{nome_cliente}_analise_recomendacao_compra.csv')
        # df_recomendacao_final.to_csv(csv_path, index=False)
        
        print(f"\nAnálise concluída com sucesso!")
        print(f"Foram analisados {len(df_recomendacao_final)} produtos.")
        
        # Mostrar resumo por criticidade
        resumo_criticidade = df_recomendacao_final['criticidade'].value_counts().to_dict()
        print("\nResumo por criticidade:")
        for nivel, quantidade in sorted(resumo_criticidade.items(), key=lambda x: ordem_criticidade.get(x[0], 999)):
            print(f"- {nivel}: {quantidade} produtos")

        # ########################################################################
        # # Exportar para banco 
        # ########################################################################
        try:
            # Reconectar ao PostgreSQL
            print("\nReconectando ao banco de dados PostgreSQL para inserir dados na tabela analítica...")
            conn = psycopg2.connect(
                host=DB_CONFIG_MALOKA['host'],
                database=database,
                user=DB_CONFIG_MALOKA['user'],
                password=DB_CONFIG_MALOKA['password'],
                port=DB_CONFIG_MALOKA['port']
            )
            
            # Criar cursor
            cursor = conn.cursor()
            
            # Verificar se a tabela já existe no esquema maloka_analytics
            cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='analise_recomendacao_compra' AND table_schema='maloka_analytics')")
            tabela_existe = cursor.fetchone()[0]
            
            if tabela_existe:
                # Verificar se as novas colunas existem na tabela
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='analise_recomendacao_compra' AND table_schema='maloka_analytics'")
                colunas_existentes = [row[0] for row in cursor.fetchall()]
                
                # Adicionar colunas que não existem ainda
                for coluna in df_recomendacao_final.columns:
                    if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                        print(f"Adicionando nova coluna: {coluna}")
                        
                        # Determinar o tipo de dados da coluna
                        dtype = df_recomendacao_final[coluna].dtype
                        if 'int' in str(dtype):
                            tipo = 'INTEGER'
                        elif 'float' in str(dtype):
                            tipo = 'NUMERIC'  # NUMERIC é melhor para precisão
                        elif 'datetime' in str(dtype):
                            tipo = 'TIMESTAMP'
                        elif 'bool' in str(dtype):
                            tipo = 'BOOLEAN'
                        else:
                            tipo = 'TEXT'
                            
                        # Executar o ALTER TABLE para adicionar a coluna
                        try:
                            cursor.execute(f'ALTER TABLE maloka_analytics.analise_recomendacao_compra ADD COLUMN "{coluna}" {tipo}')
                            conn.commit()
                            print(f"Coluna {coluna} adicionada com sucesso!")
                        except Exception as e:
                            print(f"Erro ao adicionar coluna {coluna}: {e}")
                            conn.rollback()
                
                # Limpar os dados existentes
                print("Limpando dados existentes...")
                cursor.execute("TRUNCATE TABLE maloka_analytics.analise_recomendacao_compra")
                conn.commit()
            else:
                # Criar a tabela se não existir
                print("Criando tabela analise_recomendacao_compra no esquema maloka_analytics...")
                # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
                colunas = []
                for coluna, dtype in df_recomendacao_final.dtypes.items():
                    if 'int64' in str(dtype):
                        tipo = 'BIGINT'  # Usar BIGINT para inteiros grandes
                    elif 'int' in str(dtype):
                        tipo = 'INTEGER'
                    elif 'float' in str(dtype):
                        tipo = 'NUMERIC'  # NUMERIC tem precisão exata
                    elif 'datetime' in str(dtype):
                        tipo = 'TIMESTAMP'
                    elif 'bool' in str(dtype):
                        tipo = 'BOOLEAN'  # Usar tipo booleano para flags
                    else:
                        # Limitar o tamanho de campos de texto para economizar espaço
                        if coluna in ['nome_produto', 'nome_fornecedor', 'descricao']:
                            tipo = 'VARCHAR(255)'  # Limitar texto longo
                        else:
                            tipo = 'TEXT'
                    colunas.append(f'"{coluna}" {tipo}')
                
                # Criar a tabela inicialmente como UNLOGGED para inserção mais rápida
                create_table_query = f"""
                CREATE UNLOGGED TABLE maloka_analytics.analise_recomendacao_compra (
                    {", ".join(colunas)}
                )
                """
                cursor.execute(create_table_query)
                print("Tabela UNLOGGED criada para inserção rápida")
                
                # Definir fillfactor para reduzir fragmentação
                cursor.execute("ALTER TABLE maloka_analytics.analise_recomendacao_compra SET (fillfactor = 90)")
            
            # Otimização da inserção de dados para a tabela de análise de recomendação de compra
            print(f"Inserindo {len(df_recomendacao_final)} registros na tabela analise_recomendacao_compra...")
            
            # Preparar as colunas para inserção
            colunas = [f'"{col}"' for col in df_recomendacao_final.columns]
            
            # Otimizar a tabela temporariamente para inserção rápida
            try:
                print("Otimizando configurações da tabela para inserção rápida...")
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET synchronous_commit = off")
            except Exception as e:
                print(f"Aviso ao otimizar tabela: {e}")
            
            # Método 1: Usar COPY FROM (Pull Processing)
            try:
                print("Tentando inserção usando COPY FROM (método mais rápido)...")
                
                # Criar arquivo temporário para o COPY
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    temp_path = temp_file.name
                    # Salvar DataFrame para CSV temporário
                    df_recomendacao_final.to_csv(temp_path, index=False, header=False, na_rep='\\N')
                
                # Abrir arquivo em modo de leitura
                with open(temp_path, 'r') as f:
                    # Usar COPY para inserir dados
                    cursor.copy_expert(
                        f"COPY maloka_analytics.analise_recomendacao_compra ({', '.join(colunas)}) FROM STDIN WITH CSV",
                        f
                    )
                
                # Commit uma única vez
                conn.commit()
                
                # Remover arquivo temporário
                os.unlink(temp_path)
                
                print(f"Todos os {len(df_recomendacao_final)} registros inseridos com sucesso via COPY FROM!")
                
            except Exception as e:
                print(f"Erro ao usar COPY FROM: {e}")
                print("Tentando método alternativo usando multiprocessing...")
                
                # Reverter qualquer transação pendente
                conn.rollback()
                
                # Método 2: Usar multiprocessing para INSERT paralelo
                
                # Converter NaN para None
                df_upload = df_recomendacao_final.replace({np.nan: None})
                
                # Garantir que id_produto seja inteiro
                if 'id_produto' in df_upload.columns:
                    df_upload['id_produto'] = df_upload['id_produto'].astype(int)
                
                # Preparar a query para INSERT
                placeholders = ", ".join(["%s"] * len(df_upload.columns))
                insert_query = f"""
                INSERT INTO maloka_analytics.analise_recomendacao_compra ({", ".join(colunas)})
                VALUES ({placeholders})
                """
                
                # Criar lista de tuplas com os valores
                valores = [tuple(row) for _, row in df_upload.iterrows()]
                
                # Configurar tamanho de lote baseado no número de núcleos da CPU
                num_cores = cpu_count()
                print(f"Usando {num_cores} núcleos de CPU para processamento paralelo")
                
                # Determinar tamanho do lote (dividir os dados igualmente entre cores)
                records_per_core = max(1000, len(valores) // (num_cores * 2))
                batch_size = min(10000, records_per_core)  # Limitar a 10.000 por lote
                
                # Criar lotes para processamento paralelo
                lotes = []
                for i in range(0, len(valores), batch_size):
                    batch = valores[i:i+batch_size]
                    
                    # Configuração da conexão ao banco
                    db_config = {
                        'host': DB_CONFIG_MALOKA['host'],
                        'database': database,
                        'user': DB_CONFIG_MALOKA['user'],
                        'password': DB_CONFIG_MALOKA['password'],
                        'port': DB_CONFIG_MALOKA['port']
                    }
                    
                    # Adicionar lote à lista de tarefas
                    lotes.append((batch, db_config, insert_query, i))
                
                # Iniciar processamento paralelo
                start_time = datetime.now()
                print(f"Iniciando inserção paralela com {len(lotes)} lotes...")
                
                # Usar um pool de processos
                with Pool(processes=num_cores) as pool:
                    resultados = pool.map(processar_lote, lotes)
                    
                    # Contar registros inseridos e verificar erros
                    total_inseridos = 0
                    erros = []
                    
                    for quantidade, erro in resultados:
                        total_inseridos += quantidade
                        if erro:
                            erros.append(erro)
                    
                    elapsed = (datetime.now() - start_time).total_seconds()
                    print(f"Inseridos {total_inseridos} de {len(valores)} registros em {elapsed:.2f} segundos")
                    
                    if erros:
                        print(f"Ocorreram {len(erros)} erros durante a inserção")
                        for erro in erros[:5]:  # Mostrar apenas os 5 primeiros erros
                            print(f"- {erro}")
                
                if total_inseridos < len(valores):
                    print(f"Atenção: {len(valores) - total_inseridos} registros não foram inseridos")
                else:
                    print("Todos os registros foram inseridos com sucesso!")
            
            # Restaurar configurações da tabela e otimizar para consultas
            print("Restaurando configurações e otimizando para consultas...")
            try:
                # Restaurar configurações do PostgreSQL
                cursor.execute("SET maintenance_work_mem = '64MB'")  # Valor padrão
                cursor.execute("SET synchronous_commit = on")  # Valor padrão
                
                # Converter de volta para LOGGED para garantir durabilidade
                cursor.execute("ALTER TABLE maloka_analytics.analise_recomendacao_compra SET LOGGED")
                
                # # Criar índices para melhorar performance de consultas
                # print("Criando índices para otimizar consultas futuras...")
                # cursor.execute("CREATE INDEX IF NOT EXISTS idx_recomendacao_compra_id_produto ON maloka_analytics.analise_recomendacao_compra (id_produto)")
                # cursor.execute("CREATE INDEX IF NOT EXISTS idx_recomendacao_compra_id_fornecedor ON maloka_analytics.analise_recomendacao_compra (id_fornecedor)")
                # cursor.execute("CREATE INDEX IF NOT EXISTS idx_recomendacao_compra_estoque_atual ON maloka_analytics.analise_recomendacao_compra (estoque_atual)")
                
                # Analisar tabela para otimizar planejamento de consultas
                print("Analisando tabela para otimizar consultas...")
                cursor.execute("ANALYZE maloka_analytics.analise_recomendacao_compra")
                
            except Exception as e:
                print(f"Aviso ao restaurar configurações: {e}")
            
            print(f"Dados inseridos com sucesso! Total de {len(df_recomendacao_final)} registros.")
            
            # Fechar cursor e conexão
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"\nErro ao inserir dados no banco: {e}")
            import traceback
            traceback.print_exc()
            if 'conn' in locals() and conn is not None:
                conn.close()
                
        print(f"\nRelatório de recomendação de compras gerado para {nome_cliente}")

    except Exception as e:
        print(f"\nErro durante a análise de recomendação: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Gera relatórios de faturamento para um cliente específico')
    parser.add_argument('cliente', type=str, nargs='?', default='todos', help='Nome do cliente para gerar relatórios (opcional, padrão: todos)')
    
    # Parse dos argumentos
    args = parser.parse_args()
    
    # Verificar se o usuário solicitou processamento de todos os clientes
    if args.cliente.lower() == 'todos':
        print("Gerando relatórios para todos os clientes...")
        for cliente in CLIENTES.keys():
            print("\n" + "="*50)
            print(f"Processando cliente: {cliente}")
            print("="*50)
            gerar_relatorios_compra(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_relatorios_compra(args.cliente)

"""
Para executar um cliente específico, use o comando:
python analise_compra.py nome_do_cliente

Para executar para todos os clientes, use o comando:
python analise_compra.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python analise_compra.py
"""
