from datetime import datetime
import pandas as pd
import os
import warnings
import psycopg2
import numpy as np
import argparse
import sys
import multiprocessing as mp
from functools import partial
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import DB_CONFIG_MALOKA

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Função para processar cada chunk - movida para o escopo global para permitir pickle
def process_chunk(chunk_data):
    """
    Processa um chunk de dados com fpgrowth.
    
    Args:
        chunk_data (tuple): Tuple contendo (chunk, min_support, use_colnames, total_rows)
    
    Returns:
        DataFrame: Resultados do fpgrowth com suporte ajustado
    """
    chunk, min_support, use_colnames, total_rows = chunk_data
    
    # Calcular fator de ajuste para normalizar o suporte
    chunk_size = len(chunk)
    ajuste = chunk_size / total_rows
    
    # Ajustar min_support para este chunk específico
    chunk_min_support = min_support / ajuste if ajuste > 0 else min_support
    
    # Processar com fpgrowth
    resultado = fpgrowth(chunk, min_support=chunk_min_support, use_colnames=use_colnames)
    
    # Ajustar os valores de suporte para a escala original
    if not resultado.empty:
        resultado['support'] = resultado['support'] * ajuste
        
    return resultado

def parallel_fpgrowth(df, min_support, use_colnames=True, n_chunks=None, memory_limit_gb=4):
    """
    Executa o algoritmo fpgrowth de forma paralela, dividindo o DataFrame em chunks
    e processando cada chunk em paralelo usando multiprocessing.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser analisado
        min_support (float): Suporte mínimo para o algoritmo fpgrowth
        use_colnames (bool): Se deve usar os nomes das colunas nos resultados
        n_chunks (int): Número de chunks para dividir os dados. Se None, usa o número de CPUs
        memory_limit_gb (float): Limite de memória por worker em GB
        
    Returns:
        pandas.DataFrame: DataFrame com os itemsets frequentes encontrados
    """
    # Obter o número de CPUs disponíveis (movido para fora do if para ser acessível em todo o escopo)
    n_cpus = mp.cpu_count()
    
    # Determinar número ideal de chunks com base no tamanho dos dados e recursos do sistema
    if n_chunks is None:
        # Estimar tamanho do DataFrame em memória (em bytes)
        df_size_gb = df.memory_usage(deep=True).sum() / (1024**3)
        
        # Calcular número de chunks necessários para ficar abaixo do limite de memória
        n_chunks_by_memory = max(1, int(df_size_gb / memory_limit_gb))
        
        # Usar o maior entre os dois valores para garantir eficiência e uso adequado de memória
        n_chunks = max(n_cpus, n_chunks_by_memory)
    
    print(f"Processando fpgrowth em paralelo com {n_chunks} chunks (estimativa de memória: {df.memory_usage(deep=True).sum() / (1024**3):.2f} GB)...")
    
    # Obter tamanho total do DataFrame para cálculos de ajuste de suporte
    total_rows = len(df)
    
    # Dividir o DataFrame em chunks com tamanhos balanceados
    chunk_size = max(1, len(df) // n_chunks)
    chunks = []
    for i in range(0, len(df), chunk_size):
        end = min(i + chunk_size, len(df))
        chunks.append((df.iloc[i:end], min_support, use_colnames, total_rows))
    
    # Configurar o pool com limite de processos para evitar sobrecarga de memória
    max_processes = min(n_chunks, n_cpus)
    
    # Processar chunks em paralelo
    try:
        with mp.Pool(processes=max_processes) as pool:
            results = pool.map(process_chunk, chunks)
            
    except (MemoryError, Exception) as e:
        print(f"Erro durante o processamento paralelo: {str(e)}")
        print("Tentando processar com menos processos paralelos...")
        # Caso ocorra erro, tentar com menos processos
        try:
            with mp.Pool(processes=max(1, max_processes // 2)) as pool:
                results = pool.map(process_chunk, chunks)
        except Exception as e2:
            print(f"Erro persistente durante processamento paralelo: {str(e2)}")
            print("Tentando método tradicional sem paralelização...")
            # Se continuar falhando, usar o método tradicional (não-paralelo)
            results = [fpgrowth(chunk[0], min_support=min_support, use_colnames=use_colnames) 
                      for chunk in chunks]
    
    # Combinar resultados
    if not results or all(res.empty for res in results):
        print("Nenhum resultado obtido dos chunks processados.")
        return pd.DataFrame()
        
    # Filtrar resultados vazios
    results = [res for res in results if not res.empty]
    
    if not results:
        return pd.DataFrame()
        
    # Combinar todos os DataFrames de resultados e agrupar itemsets idênticos
    combined_df = pd.concat(results)
    
    # Agrupar por itemsets e somar os suportes
    if not combined_df.empty and 'itemsets' in combined_df.columns:
        # Convertemos itemsets para string para poder agrupar
        combined_df['itemsets_str'] = combined_df['itemsets'].astype(str)
        
        # Agrupar e calcular a média ponderada dos suportes
        grouped = combined_df.groupby('itemsets_str')['support'].mean().reset_index()
        
        # Converter itemsets_str de volta para frozenset (precisa de função helper)
        def str_to_frozenset(s):
            try:
                # Remover caracteres de frozenset e formatar para eval
                s = s.replace('frozenset(', '').replace(')', '')
                return frozenset(eval(s))
            except:
                return frozenset()
                
        grouped['itemsets'] = grouped['itemsets_str'].apply(str_to_frozenset)
        grouped = grouped.drop('itemsets_str', axis=1)
        
        # Filtrar pelo suporte mínimo definido
        combined_results = grouped[grouped['support'] >= min_support]
    else:
        combined_results = combined_df.drop_duplicates()
        
        # Remover itemsets com suporte abaixo do mínimo
        if not combined_results.empty and 'support' in combined_results.columns:
            combined_results = combined_results[combined_results['support'] >= min_support]
    
    return combined_results

def gerar_relatorios_cross_selling(nome_cliente):
    """
    Gera relatórios de cross-selling para o cliente especificado.
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
    tempo_analise_anos = config_cliente["tempo_analise_anos"]
    usar_cross_selling_produtos = config_cliente["usar_cross_selling_produtos"]
    cliente_min_support = config_cliente["min_support"]
    cliente_min_confidence = config_cliente["min_confidence"]
    podar_populares = config_cliente["podar_populares"]
    quantidade_populares = config_cliente["quantidade_populares"]

    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    
    # Criar diretório para salvar os relatórios do cliente
    diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_cross_selling', nome_cliente)
    os.makedirs(diretorio_cliente, exist_ok=True)
    
    print(f"Gerando relatórios para o cliente: {nome_cliente}")
    print(f"Database: {database}, Schema: {schema}")
    print("Nota: id_venda será tratado como longint (int64) durante todo o processamento")

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
        SELECT id_venda, id_cliente, data_venda, total_venda, id_loja
        FROM {schema}.venda
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_vendas = pd.read_sql_query(query, conn)
        
        # Converter id_venda para longint (int64)
        df_vendas['id_venda'] = df_vendas['id_venda'].astype('int64')
        
        # Informações sobre os dados
        num_registros = len(df_vendas)
        num_colunas = len(df_vendas.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_vendas.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_vendas.head())
        
        # Exportar para CSV
        # df_vendas.to_csv("df_vendas_BD.csv", index=False)

        ########################################################
        # consulta da tabela venda_itens
        ########################################################
        
        print("Consultando a tabela VENDA_ITEM...")

        query = f"""
        SELECT id_venda_item, id_venda, id_produto, quantidade, preco_bruto, tipo
        FROM {schema}.venda_item
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_venda_itens = pd.read_sql_query(query, conn)
        
        # Converter id_venda para longint (int64)
        df_venda_itens['id_venda'] = df_venda_itens['id_venda'].astype('int64')
        
        # Informações sobre os dados
        num_registros = len(df_venda_itens)
        num_colunas = len(df_venda_itens.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_venda_itens.columns)}")
        
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
        SELECT id_produto, nome AS nome_produto, id_categoria, codigo AS codigo_produto, codigo_barras
        FROM {schema}.produto
        """

        # Carregar os dados diretamente em um DataFrame do pandas
        df_produto = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_produto)
        num_colunas = len(df_produto.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_produto.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_produto.head())
        
        # Exportar para CSV
        # df_produto.to_csv("df_produto_BD.csv", index=False)

        ########################################################
        # consulta da tabela categoria
        ########################################################

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
        print(f"Colunas disponíveis: {', '.join(df_categoria.columns)}")
            
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_categoria.head())

        # Exportar para CSV
        # df_categoria.to_csv("df_categoria_BD.csv", index=False)

        ########################################################
        # consulta da tabela historico_estoque
        ########################################################

        print("Consultando a tabela HISTORICO_ESTOQUE...")
        query = f"""
        SELECT id_produto, data_estoque, estoque AS estoque_atual
        FROM {schema}.historico_estoque
        """ 
        # Carregar os dados diretamente em um DataFrame do pandas
        df_historico_estoque = pd.read_sql_query(query, conn)

        # Informações sobre os dados
        num_registros = len(df_historico_estoque)
        num_colunas = len(df_historico_estoque.columns)  

        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_historico_estoque.columns)}")
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_historico_estoque.head())

        # Exportar para CSV
        # df_historico_estoque.to_csv("df_historico_estoque_BD.csv", index=False)

        ########################################################
        # consulta da tabela cliente
        ########################################################

        print("Consultando a tabela CLIENTE...")
        # Colunas disponíveis: id_cliente, tipo, nome, email, telefone, endereco, cep, cidade, estado, identificador_compartilhado, data_cadastro
        query = f"""
        SELECT id_cliente, nome AS nome_cliente 
        FROM {schema}.cliente
        """
        # Carregar os dados diretamente em um DataFrame do pandas
        df_cliente = pd.read_sql_query(query, conn)

        # Informações sobre os dados
        num_registros = len(df_cliente)
        num_colunas = len(df_cliente.columns)   

        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_cliente.columns)}")
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_cliente.head())

        # Exportar para CSV
        # df_cliente.to_csv("df_cliente_BD.csv", index=False)

        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")

    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")

    # Pegar somente vendas no ano especificado
    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
    #filtrar df_vendas para somente vendas que possuem a tipo_venda como PEDIDO e situacao_venda como CONCLUIDA
    df_vendas = df_vendas[(df_vendas['tipo_venda'] == 'PEDIDO') & (df_vendas['situacao_venda'] == 'CONCLUIDA')]
    data_limite = pd.Timestamp.now() - pd.DateOffset(years= tempo_analise_anos)
    # tempo da analise
    print(f"Analise de vendas do último(s) {tempo_analise_anos} ano(s)")
    
    # Informações de debug sobre as datas
    print(f"Data limite para filtragem: {data_limite}")
    print(f"Data mais antiga na base: {df_vendas['data_venda'].min()}")
    print(f"Data mais recente na base: {df_vendas['data_venda'].max()}")
    print(f"Total de vendas antes do filtro: {len(df_vendas)}")
    
    # Filtro por data
    df_vendas = df_vendas[df_vendas['data_venda'] >= data_limite]
    
    # Verifica resultados após filtragem
    print(f"Total de vendas após filtro de data: {len(df_vendas)}")
    if len(df_vendas) > 0:
        print(f"Nova data mais antiga após filtro: {df_vendas['data_venda'].min()}")
    
    # Pegar somente os itens com tipo venda igual a P
    df_venda_itens = df_venda_itens[df_venda_itens['tipo'] == 'P']
    
    # Filtrar itens para considerar apenas as vendas do período selecionado
    vendas_filtradas = df_vendas['id_venda'].unique()
    print(f"Total de id_vendas após filtro de data: {len(vendas_filtradas)}")
    print(f"Total de itens antes do filtro por data: {len(df_venda_itens)}")
    df_venda_itens = df_venda_itens[df_venda_itens['id_venda'].isin(vendas_filtradas)]
    print(f"Total de itens após filtro por data: {len(df_venda_itens)}")

    # Analisar quantas vendas têm apenas um item
    vendas_por_quantidade_itens = df_venda_itens.groupby('id_venda').size().value_counts()
    total_vendas = df_venda_itens['id_venda'].nunique()
    
    # Verificar quantas vendas têm apenas um item
    vendas_um_item = vendas_por_quantidade_itens.get(1, 0)
    percentual_um_item = (vendas_um_item / total_vendas) * 100 if total_vendas > 0 else 0
    
    print("\nAnálise de composição das vendas:")
    print(f"Total de vendas distintas: {total_vendas}")
    print(f"Vendas com apenas um item: {vendas_um_item} ({percentual_um_item:.2f}%)")
    print(f"Vendas com múltiplos itens: {total_vendas - vendas_um_item} ({100 - percentual_um_item:.2f}%)")
    print("Distribuição de vendas por quantidade de itens:")
    for qtd, freq in vendas_por_quantidade_itens.items():
        print(f"  {qtd} item(s): {freq} vendas ({(freq/total_vendas)*100:.2f}%)")

    # Remover vendas que contêm apenas um item
    vendas_multiplos_itens = df_venda_itens.groupby('id_venda').size()
    vendas_validas = vendas_multiplos_itens[vendas_multiplos_itens > 1].index
    df_venda_itens = df_venda_itens[df_venda_itens['id_venda'].isin(vendas_validas)]
    
    print(f"Total de vendas após filtro (apenas vendas com múltiplos itens): {len(vendas_validas)}")
    print(f"Total de itens após filtro: {len(df_venda_itens)}")

    # juntar tabela venda com venda_itens
    df_vendas_completo = pd.merge(
        df_vendas, 
        df_venda_itens, 
        on='id_venda', 
        how='inner'
    )
    print("\nDataFrame de Vendas Completo:")
    print(df_vendas_completo.head())

    # Adicionar dados de produto ao dataframe de vendas completo
    df_vendas_completo = pd.merge(
        df_vendas_completo,
        df_produto[['id_produto', 'nome_produto', 'id_categoria', 'codigo_produto', 'codigo_barras']],
        on='id_produto',
        how='inner'
    )

    # Adicionar dados de cliente ao dataframe de vendas completo
    df_vendas_completo = pd.merge(
        df_vendas_completo,
        df_cliente,
        on='id_cliente',
        how='inner'
    )

    #colocando categoria nos produtos
    df_produto = df_produto.merge(
        df_categoria[['id_categoria', 'nome_categoria']],
        on='id_categoria',
        how='left'
    )
    
    print("\nDataFrame de Vendas Completo com Produtos:")
    # print(df_vendas_completo.columns)
    print(df_vendas_completo.head())

    def calcular_cobertura_e_metadata(rules, df_vendas_completo, df_historico_estoque, tipo_analise):
        """
        Função para calcular cobertura e metadata que funciona para ambos os tipos de análise
        """
        if rules.empty:
            return pd.DataFrame()
        
        # Calcular cobertura apenas dos SKUs ativos no último ano
        data_limite_sku_ativo = pd.Timestamp.now() - pd.DateOffset(years=1)
        df_vendas_ultimo_ano = df_vendas_completo[df_vendas_completo['data_venda'] >= data_limite_sku_ativo]
        skus_ativos_ultimo_ano = df_vendas_ultimo_ano['id_produto'].nunique()
        
        # Calcular cobertura de SKUs
        skus_nas_regras = set(rules['produto_base'].unique()) | set(rules['produto_recomendado'].unique())
        skus_ativos_nas_regras = set(skus_nas_regras) & set(df_vendas_ultimo_ano['id_produto'].unique())
        cobertura_skus = (len(skus_ativos_nas_regras) / skus_ativos_ultimo_ano) * 100 if skus_ativos_ultimo_ano > 0 else 0
        
        # Produtos antecedentes (base) e consequentes (recomendados) nas regras
        produtos_base = rules['produto_base'].unique()
        produtos_recomendados = rules['produto_recomendado'].unique()

        # Filtrar apenas produtos base que são ativos
        produtos_base_ativos = [p for p in produtos_base if p in df_vendas_ultimo_ano['id_produto'].unique()]
        
        # Filtrar apenas produtos recomendados que são ativos
        produtos_recomendados_ativos = [p for p in produtos_recomendados if p in df_vendas_ultimo_ano['id_produto'].unique()]
        
        
        # Obter os dados de estoque mais recentes para cada produto
        # Converter data_estoque para datetime se ainda não for
        df_historico_estoque['data_estoque'] = pd.to_datetime(df_historico_estoque['data_estoque'])
        
        # Obter estoque mais recente para cada produto
        estoque_atual = df_historico_estoque.sort_values('data_estoque').drop_duplicates('id_produto', keep='last')
        
        # Criar um dicionário de id_produto para estoque_atual
        dict_estoque = estoque_atual.set_index('id_produto')['estoque_atual'].to_dict()
        
        # Verificar cobertura de estoque para produtos base ativos
        produtos_base_com_estoque = [p for p in produtos_base_ativos if p in dict_estoque and dict_estoque[p] > 0]
        cobertura_estoque_base = (len(produtos_base_com_estoque) / len(produtos_base_ativos)) * 100 if len(produtos_base_ativos) > 0 else 0
        
        # Verificar cobertura de estoque para produtos recomendados ativos
        produtos_recomendados_com_estoque = [p for p in produtos_recomendados_ativos if p in dict_estoque and dict_estoque[p] > 0]
        cobertura_estoque_recomendados = (len(produtos_recomendados_com_estoque) / len(produtos_recomendados_ativos)) * 100 if len(produtos_recomendados_ativos) > 0 else 0
        
        print(f"\nTotal de SKUs ativos (último ano): {skus_ativos_ultimo_ano}")
        print(f"\nCobertura de SKUs nas regras: {cobertura_skus:.2f}% ({len(skus_ativos_nas_regras)} de {skus_ativos_ultimo_ano} SKUs)")
        print(f"Cobertura de estoque dos produtos base: {cobertura_estoque_base:.2f}% ({len(produtos_base_com_estoque)} de {len(produtos_base_ativos)} produtos com estoque > 0)")
        print(f"Cobertura de estoque dos produtos recomendados: {cobertura_estoque_recomendados:.2f}% ({len(produtos_recomendados_com_estoque)} de {len(produtos_recomendados_ativos)} produtos com estoque > 0)")
        
        # Criar metadata
        metadata = pd.DataFrame([{
            'data_execucao': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tipo_analise': tipo_analise,
            'total_skus_ativos': skus_ativos_ultimo_ano,
            'skus_nas_regras': len(skus_ativos_nas_regras),
            'cobertura_percentual_regras': cobertura_skus,
            'total_produtos_base': len(produtos_base_ativos),
            'produtos_base_com_estoque': len(produtos_base_com_estoque),
            'cobertura_estoque_base': cobertura_estoque_base,
            'total_produtos_recomendados': len(produtos_recomendados_ativos),
            'produtos_recomendados_com_estoque': len(produtos_recomendados_com_estoque),
            'cobertura_estoque_recomendados': cobertura_estoque_recomendados,
            'poda_produtos_populares': podar_populares,
            'quantidade_produtos_podados': quantidade_populares if podar_populares else 0
        }])
        
        return metadata

    def create_customer_based_recommendations(df_vendas_completo, min_support=cliente_min_support, min_confidence=cliente_min_confidence):
        """
        Análise de Market Basket baseada em clientes - HISTÓRICO CONSOLIDADO
        
        Cada cliente vira um "ticket" único contendo TODOS os produtos que já comprou.
        Isso permite encontrar semelhanças entre perfis de clientes e gerar recomendações
        baseadas em "clientes que compraram produtos similares aos seus também compraram..."
        """
        print("=== ANÁLISE BASEADA EM CLIENTES - HISTÓRICO CONSOLIDADO ===")
        print("Consolidando todo o histórico de compras por cliente...")
        
        # PASSO 1: Consolidar todos os produtos comprados por cada cliente
        # Cada linha será um cliente e suas colunas serão os produtos que já comprou (True/False)
        
        # Obter produtos únicos por cliente (remove duplicatas)
        produtos_por_cliente = df_vendas_completo.groupby('id_cliente')['id_produto'].apply(
            lambda x: x.unique().tolist()
        ).reset_index()
        
        print(f"Total de clientes analisados: {len(produtos_por_cliente)}")
        
        # Estatísticas do histórico consolidado
        produtos_por_cliente['qtd_produtos_distintos'] = produtos_por_cliente['id_produto'].apply(len)
        
        print(f"Média de produtos distintos por cliente: {produtos_por_cliente['qtd_produtos_distintos'].mean():.2f}")
        print(f"Mediana de produtos distintos por cliente: {produtos_por_cliente['qtd_produtos_distintos'].median():.2f}")
        print(f"Cliente com mais produtos: {produtos_por_cliente['qtd_produtos_distintos'].max()} produtos")
        print(f"Cliente com menos produtos: {produtos_por_cliente['qtd_produtos_distintos'].min()} produtos")
        
        # Criar diretamente a matriz usando drop_duplicates e pivot_table
        print("Criando matriz otimizada de clientes x produtos...")
        df_unique = df_vendas_completo[['id_cliente', 'id_produto']].drop_duplicates()
        df_unique['comprou'] = True
        
        # Criar matriz pivot: clientes x produtos
        basket_consolidado = df_unique.pivot_table(
            index='id_cliente', 
            columns='id_produto', 
            values='comprou', 
            fill_value=False
        )
        
        print(f"\nMatriz criada: {len(basket_consolidado)} clientes x {len(basket_consolidado.columns)} produtos")
        
        # Limitar a 50 produtos por cliente
        print(f"Limitando a um máximo de 50 produtos por cliente dos {len(basket_consolidado.columns)} totais")
        for cliente_idx in basket_consolidado.index:
            produtos_do_cliente = basket_consolidado.loc[cliente_idx]
            # Obter produtos que são True (comprados pelo cliente)
            produtos_comprados = [col for col in basket_consolidado.columns if produtos_do_cliente[col] == True]
            
            # Se o cliente comprou mais de 50 produtos, limitar para 50
            if len(produtos_comprados) > 50:
                # Produtos para manter (primeiros 50)
                produtos_para_manter = produtos_comprados[:50]
                
                # Zerar todos os produtos primeiro
                basket_consolidado.loc[cliente_idx, :] = False
                
                # Depois definir como True apenas os 50 produtos selecionados
                basket_consolidado.loc[cliente_idx, produtos_para_manter] = True
        
        # Análise de densidade da matriz
        total_celulas = len(basket_consolidado) * len(basket_consolidado.columns)
        celulas_verdadeiras = basket_consolidado.sum().sum()
        densidade = (celulas_verdadeiras / total_celulas) * 100
        
        print(f"Densidade da matriz: {densidade:.4f}% ({celulas_verdadeiras:,} de {total_celulas:,} células)")
        
        # PASSO 4: Aplicar FP-Growth para encontrar padrões entre clientes
        print("\nAplicando FP-Growth em paralelo para encontrar produtos frequentemente comprados juntos por clientes similares...")
        
        try:
            # Usar a versão paralela do fpgrowth para otimizar o processamento
            start_time = datetime.now()
            print(f"Iniciando processamento paralelo em {start_time}")
            
            # Determinar o número de chunks baseado no tamanho dos dados
            n_chunks_auto = min(mp.cpu_count(), max(1, len(basket_consolidado) // 1000))
            
            # Encontrar itemsets frequentes usando multiprocessing
            frequent_itemsets = parallel_fpgrowth(
                basket_consolidado, 
                min_support=min_support, 
                use_colnames=True,
                n_chunks=n_chunks_auto
            )
            
            end_time = datetime.now()
            processing_time = end_time - start_time
            print(f"Tempo de processamento do fpgrowth: {processing_time}")
            
            if frequent_itemsets.empty:
                print(f"Nenhum itemset frequente encontrado com min_support={min_support}")
                print("Sugestões:")
                print("1. Reduza o valor de min_support")
                print("2. Verifique se há clientes suficientes com produtos em comum")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Erro durante o processamento paralelo: {str(e)}")
            print("Tentando método tradicional sem paralelização...")
            
            try:
                # Fallback para o método tradicional em caso de erro
                print("Executando fpgrowth sem paralelização...")
                frequent_itemsets = fpgrowth(basket_consolidado, min_support=min_support, use_colnames=True)
                
                if frequent_itemsets.empty:
                    print("Nenhum itemset frequente encontrado. Experimente reduzir o valor de min_support.")
                    return pd.DataFrame()
            except Exception as e2:
                print(f"Erro também no método tradicional: {str(e2)}")
                print("Não foi possível encontrar itemsets frequentes. Verifique os parâmetros ou os dados.")
                return pd.DataFrame()
            
            print(f"Itemsets frequentes encontrados: {len(frequent_itemsets)}")
            
            # Gerar regras de associação
            rules_df = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
            
            if rules_df.empty:
                print(f"Nenhuma regra de associação encontrada com min_confidence={min_confidence}")
                return pd.DataFrame()
            
            # Filtrar apenas regras com um antecedente e um consequente
            rules_df = rules_df[rules_df['antecedents'].apply(len) == 1]
            rules_df = rules_df[rules_df['consequents'].apply(len) == 1]
            
            if rules_df.empty:
                print("Nenhuma regra simples (1 produto -> 1 produto) encontrada")
                return pd.DataFrame()
            
            # PASSO 5: Processar e formatar resultados
            rules_df['produto_base'] = rules_df['antecedents'].apply(lambda x: list(x)[0])
            rules_df['produto_recomendado'] = rules_df['consequents'].apply(lambda x: list(x)[0])
            
            # Modificar o cálculo de estatísticas dos clientes que aparece no código
            rules_df['qtd_clientes_produto_base'] = rules_df['produto_base'].apply(
                lambda x: basket_consolidado[x].astype(bool).sum()
            )
            rules_df['qtd_clientes_produto_recomendado'] = rules_df['produto_recomendado'].apply(
                lambda x: basket_consolidado[x].astype(bool).sum()
            )
            rules_df['qtd_clientes_ambos_produtos'] = rules_df.apply(
                lambda row: (basket_consolidado[row['produto_base']].astype(bool) & basket_consolidado[row['produto_recomendado']].astype(bool)).sum(), 
                axis=1
            )
            
            # Calcular percentuais
            total_clientes = len(basket_consolidado)
            rules_df['perc_clientes_produto_base'] = (rules_df['qtd_clientes_produto_base'] / total_clientes) * 100
            rules_df['perc_clientes_produto_recomendado'] = (rules_df['qtd_clientes_produto_recomendado'] / total_clientes) * 100
            rules_df['perc_clientes_ambos_produtos'] = (rules_df['qtd_clientes_ambos_produtos'] / total_clientes) * 100
            
            # Renomear colunas
            rules_df = rules_df.rename(columns={
                'support': 'suporte',
                'confidence': 'confianca', 
                'lift': 'lift'
            })
            
            # Selecionar colunas finais
            result_df = rules_df[[
                'produto_base',
                'produto_recomendado', 
                'suporte', 
                'confianca',
                'lift',
                'qtd_clientes_produto_base',
                'qtd_clientes_produto_recomendado', 
                'qtd_clientes_ambos_produtos',
                'perc_clientes_produto_base',
                'perc_clientes_produto_recomendado',
                'perc_clientes_ambos_produtos'
            ]].sort_values('confianca', ascending=False)
            
            print(f"\nRegras de associação geradas: {len(result_df)}")
            print(f"Interpretação: 'Clientes que compraram o produto X também compraram o produto Y'")
            
            # Estatísticas das regras
            print(f"\nEstatísticas das regras:")
            print(f"Lift médio: {result_df['lift'].mean():.2f}")
            print(f"Confiança média: {result_df['confianca'].mean():.3f} ({result_df['confianca'].mean()*100:.1f}%)")
            print(f"Suporte médio: {result_df['suporte'].mean():.4f} ({result_df['suporte'].mean()*100:.2f}%)")
            
            return result_df
            
        except Exception as e:
            print(f"Erro durante a análise: {str(e)}")
            return pd.DataFrame()

    def create_product_based_recommendations(df_venda_itens, min_support=cliente_min_support, min_confidence=cliente_min_confidence):
        """
        Análise tradicional de Market Basket - produtos comprados juntos na mesma transação
        """
        print("=== ANÁLISE BASEADA EM PRODUTOS ===")
        print("Analisando produtos comprados juntos nas mesmas transações...")
        
        # Criar matriz de produtos por pedido (one-hot encoding)
        basket = pd.crosstab(df_venda_itens['id_venda'], df_venda_itens['id_produto'])
        basket_bool = basket.applymap(lambda x: True if x > 0 else False)
        
        print(f"Total de transações: {len(basket_bool)}")
        print(f"Total de produtos únicos: {len(basket_bool.columns)}")
        
        # Aplicar FP-Growth para encontrar itemsets frequentes
        try:
            # Usar a versão paralela do fpgrowth para otimizar o processamento
            start_time = datetime.now()
            print(f"Iniciando processamento paralelo em {start_time}")
            
            # Determinar o número de chunks baseado no tamanho dos dados
            n_chunks_auto = min(mp.cpu_count(), max(1, len(basket_bool) // 1000))
            
            # Encontrar itemsets frequentes usando multiprocessing
            frequent_itemsets = parallel_fpgrowth(
                basket_bool, 
                min_support=min_support, 
                use_colnames=True,
                n_chunks=n_chunks_auto
            )
            
            end_time = datetime.now()
            processing_time = end_time - start_time
            print(f"Tempo de processamento do fpgrowth: {processing_time}")
            
            if frequent_itemsets.empty:
                print(f"Nenhum itemset frequente encontrado com min_support={min_support}")
                print("Sugestões:")
                print("1. Reduza o valor de min_support")
                print("2. Verifique se há transações suficientes com produtos em comum")
                return pd.DataFrame()
        
        except Exception as e:
            print(f"Erro durante o processamento paralelo: {str(e)}")
            print("Tentando método tradicional sem paralelização...")
            
            try:
                # Fallback para o método tradicional em caso de erro
                print("Executando fpgrowth sem paralelização...")
                frequent_itemsets = fpgrowth(basket_bool, min_support=min_support, use_colnames=True)
            except Exception as e2:
                print(f"Erro também no método tradicional: {str(e2)}")
                print("Não foi possível encontrar itemsets frequentes. Verifique os parâmetros ou os dados.")
                return pd.DataFrame()
            
            if frequent_itemsets.empty:
                print("Nenhum itemset frequente encontrado. Experimente reduzir o valor de min_support.")
                return pd.DataFrame()
        
        # Gerar regras de associação
        rules_df = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
        
        # Filtrar apenas regras com um antecedente e um consequente
        rules_df = rules_df[rules_df['antecedents'].apply(len) == 1]
        rules_df = rules_df[rules_df['consequents'].apply(len) == 1]
        
        if rules_df.empty:
            return pd.DataFrame()
        
        # Processar resultados
        rules_df['produto_base'] = rules_df['antecedents'].apply(lambda x: list(x)[0])
        rules_df['produto_recomendado'] = rules_df['consequents'].apply(lambda x: list(x)[0])
        
        # Calcular frequências
        rules_df['freq_produto_base'] = rules_df['produto_base'].apply(lambda x: basket_bool[x].sum())
        rules_df['freq_produto_recomendado'] = rules_df['produto_recomendado'].apply(lambda x: basket_bool[x].sum())
        rules_df['freq_ambos_produtos'] = rules_df.apply(lambda row: 
            (basket_bool[row['produto_base']] & basket_bool[row['produto_recomendado']]).sum(), axis=1)
        
        # Renomear colunas
        rules_df = rules_df.rename(columns={
            'support': 'suporte',
            'confidence': 'confianca',
            'lift': 'lift'
        })
        
        result_df = rules_df[[
            'produto_base', 
            'produto_recomendado', 
            'suporte', 
            'confianca', 
            'lift', 
            'freq_produto_base', 
            'freq_produto_recomendado', 
            'freq_ambos_produtos'
        ]].sort_values('confianca', ascending=False)
        
        print(f"\nRegras de associação geradas: {len(result_df)}")
        print(f"Interpretação: 'Produtos frequentemente comprados juntos na mesma transação'")
        
        # Estatísticas das regras
        print(f"\nEstatísticas das regras:")
        print(f"Lift médio: {result_df['lift'].mean():.2f}")
        print(f"Confiança média: {result_df['confianca'].mean():.3f} ({result_df['confianca'].mean()*100:.1f}%)")
        print(f"Suporte médio: {result_df['suporte'].mean():.4f} ({result_df['suporte'].mean()*100:.2f}%)")
        
        return result_df
    # Criar dicionários para mapear id_produto para nome_produto e categorias
    produto_nomes = df_produto.drop_duplicates('id_produto').set_index('id_produto')['nome_produto'].to_dict()
    produto_categorias = df_produto.drop_duplicates('id_produto').set_index('id_produto')['nome_categoria'].to_dict()
        
    def identificar_produtos_populares(df_vendas_completo, quantidade_populares):
        """
        Identifica os produtos mais populares (mais vendidos) para poda
        """
        # Contar frequência de cada produto
        produto_frequencia = df_vendas_completo['id_produto'].value_counts()
        
        # Obter os N produtos mais populares
        produtos_populares = produto_frequencia.head(quantidade_populares).index.tolist()
        
        print(f"\nIdentificados {len(produtos_populares)} produtos populares para poda:")
        if len(produtos_populares) > 0:
            for idx, produto_id in enumerate(produtos_populares):
                nome = produto_nomes.get(produto_id, f"Produto {produto_id}")
                freq = produto_frequencia[produto_id]
                print(f"  {idx+1}. {nome} (ID: {produto_id}) - {freq} ocorrências")
                
        return produtos_populares
    
    # Executar análise
    try:
        print("Iniciando análise de cross-selling...")
        
        # Identificar produtos populares se necessário
        produtos_para_podar = []
        if podar_populares and quantidade_populares > 0:
            print(f"\nAtivada poda de produtos populares. Serão removidos os {quantidade_populares} produtos mais frequentes.")
            produtos_para_podar = identificar_produtos_populares(df_vendas_completo, quantidade_populares)
        
        tipo_analise = ""
        if usar_cross_selling_produtos:
            print("Executando análise baseada em PRODUTOS...")
            # Filtrar venda_itens para remover produtos populares se necessário
            if podar_populares and len(produtos_para_podar) > 0:
                df_venda_itens_filtrado = df_venda_itens[~df_venda_itens['id_produto'].isin(produtos_para_podar)]
                print(f"Removidos {len(df_venda_itens) - len(df_venda_itens_filtrado)} itens de produtos populares da análise")
                rules = create_product_based_recommendations(df_venda_itens_filtrado, cliente_min_support, cliente_min_confidence)
            else:
                rules = create_product_based_recommendations(df_venda_itens, cliente_min_support, cliente_min_confidence)
            tipo_analise = "Produtos"
        else:
            print("Executando análise baseada em CLIENTES...")
            # Filtrar vendas_completo para remover produtos populares se necessário
            if podar_populares and len(produtos_para_podar) > 0:
                df_vendas_completo_filtrado = df_vendas_completo[~df_vendas_completo['id_produto'].isin(produtos_para_podar)]
                print(f"Removidos {len(df_vendas_completo) - len(df_vendas_completo_filtrado)} registros de produtos populares da análise")
                rules = create_customer_based_recommendations(df_vendas_completo_filtrado, cliente_min_support, cliente_min_confidence)
            else:
                rules = create_customer_based_recommendations(df_vendas_completo, cliente_min_support, cliente_min_confidence)
            tipo_analise = "Clientes"

        if not rules.empty: 
            # Adicionar descrições dos produtos e categorias
            rules['nome_produto_base'] = rules['produto_base'].map(produto_nomes)
            rules['categoria_produto_base'] = rules['produto_base'].map(produto_categorias)
            rules['nome_produto_recomendado'] = rules['produto_recomendado'].map(produto_nomes)
            rules['categoria_produto_recomendado'] = rules['produto_recomendado'].map(produto_categorias)
                    
            # Formatar resultados
            rules['suporte'] = rules['suporte'].round(5)
            rules['confianca'] = rules['confianca'].round(2)
            rules['lift'] = rules['lift'].round(2)
            # Formatar colunas específicas dependendo do tipo de análise
            if 'freq_produto_base' in rules.columns:
                rules['freq_produto_base'] = rules['freq_produto_base'].round(5)
                rules['freq_produto_recomendado'] = rules['freq_produto_recomendado'].round(5)
                rules['freq_ambos_produtos'] = rules['freq_ambos_produtos'].round(5)
            
            # Usar a função centralizada para calcular cobertura e metadata
            metadata = calcular_cobertura_e_metadata(rules, df_vendas_completo, df_historico_estoque, tipo_analise)

            # Salvar resultados no diretorio do cliente
            rules.to_csv(os.path.join(diretorio_cliente, f"{nome_cliente}_cross_selling_rules.csv"), index=False)
            metadata.to_csv(os.path.join(diretorio_cliente, f"{nome_cliente}_cross_selling_metadata.csv"), index=False)
            
            # Se houve poda por produtos populares, salvar lista de produtos podados
            if podar_populares and len(produtos_para_podar) > 0:
                # Criar dataframe com produtos podados e suas informações
                df_podados = pd.DataFrame({
                    'id_produto': produtos_para_podar
                })
                # Adicionar informações dos produtos
                df_podados['nome_produto'] = df_podados['id_produto'].map(produto_nomes)
                df_podados['categoria'] = df_podados['id_produto'].map(produto_categorias)
                df_podados['contagem'] = df_podados['id_produto'].apply(
                    lambda x: df_vendas_completo[df_vendas_completo['id_produto'] == x].shape[0]
                )
                # Salvar produtos podados
                df_podados.to_csv(os.path.join(diretorio_cliente, f"{nome_cliente}_produtos_podados.csv"), index=False)

            # print("\nTop 10 Recomendações:")
            # print(rules.head(10))
            print(f"\nTotal de regras geradas: {len(rules)}")
            print(f"Média de Lift: {rules['lift'].mean():.2f}")
            print(f"Média de Confiança: {(rules['confianca'] * 100).mean():.2f}%")
        else:
            print("Nenhuma regra encontrada com os critérios especificados")
    except Exception as e:
        print(f"Erro: {str(e)}")

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
            gerar_relatorios_cross_selling(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_relatorios_cross_selling(args.cliente)

"""
Para executar um cliente específico, use o comando:
python cross_selling.py nome_do_cliente

Para executar para todos os clientes, use o comando:
python cross_selling.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python cross_selling.py
"""