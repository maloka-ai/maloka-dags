import pandas as pd
import psycopg2
import os
import warnings
from datetime import datetime, timedelta
import sys
import numpy as np
import argparse
import multiprocessing
from multiprocessing import Pool, cpu_count
import io
import tempfile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import DB_CONFIG_MALOKA

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

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

def gerar_relatorios_estoque(nome_cliente):
    """
    Gera relatórios de estoque para um cliente específico ou todos.
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
    # diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_estoque', nome_cliente)
    # os.makedirs(diretorio_cliente, exist_ok=True)
    
    print(f"Gerando relatórios para o cliente: {nome_cliente}")
    print(f"Database: {database}, Schema: {schema}")

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
        query = f"SELECT * FROM {schema}.venda"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_vendas = pd.read_sql_query(query, conn)
        
        # Converter id_venda para longint (int64)
        if 'id_venda' in df_vendas.columns:
            df_vendas['id_venda'] = df_vendas['id_venda'].astype('int64')
        
        # Informações sobre os dados
        num_registros = len(df_vendas)
        num_colunas = len(df_vendas.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_vendas.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_vendas.head())
        
        # EXPORTAR EXCEL
        # df_vendas.to_excel("df_vendas.xlsx", index=False)
        
        ########################################################
        # consulta da tabela vendas_itens
        ########################################################
        
        print("Consultando a tabela VENDA_ITENS...")
        query = f"SELECT * FROM {schema}.venda_item"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_venda_itens = pd.read_sql_query(query, conn)
        
        # Converter id_venda para longint (int64)
        if 'id_venda' in df_venda_itens.columns:
            df_venda_itens['id_venda'] = df_venda_itens['id_venda'].astype('int64')
        
        # Informações sobre os dados
        num_registros = len(df_venda_itens)
        num_colunas = len(df_venda_itens.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_venda_itens.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_venda_itens.head())
        
        # EXPORTAR EXCEL
        # df_venda_itens.to_excel("df_venda_itens.xlsx", index=False)

        ########################################################
        # consulta da tabela estoque_movimentos
        ########################################################
        
        # Consultar a tabela estoque_movimentos
        print("Consultando a tabela ESTOQUE_MOVIMENTOS...")
        query = f"SELECT * FROM {schema}.estoque_movimento"

        # Carregar os dados diretamente em um DataFrame do pandas
        df_estoque_movi = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_estoque_movi)
        num_colunas = len(df_estoque_movi.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_estoque_movi.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_estoque_movi.head())
        
        # EXPORTAR EXCEL
        # df_estoque_movi.to_excel("df_estoque_movimentos.xlsx", index=False)

        ########################################################
        # consulta da tabela estoque
        ########################################################
        
        # Consultar a tabela estoque
        print("Consultando a tabela ESTOQUE...")
        query = f"SELECT * FROM {schema}.historico_estoque"

        # Carregar os dados diretamente em um DataFrame do pandas
        df_estoque = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_estoque)
        num_colunas = len(df_estoque.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_estoque.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_estoque.head())
        
        # EXPORTAR EXCEL
        # df_estoque.to_excel("df_estoque.xlsx", index=False)

        ########################################################
        # consulta da tabela produtos
        ########################################################
        
        # Consultar a tabela produtos
        print("Consultando a tabela PRODUTOS...")
        query = f"SELECT * FROM {schema}.produto"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_produtos = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_produtos)
        num_colunas = len(df_produtos.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_produtos.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_produtos.head())
        
        # EXPORTAR EXCEL
        # df_produtos.to_excel("df_produtos.xlsx", index=False)

        ########################################################
        # consulta da tabela categoria
        ########################################################
        
        # Consultar a tabela categotia
        print("Consultando a tabela PRODUTOS...")
        query = f"SELECT * FROM {schema}.categoria"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_categoria = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_categoria)
        num_colunas = len(df_categoria.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_categoria.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_categoria.head())
        
        # EXPORTAR EXCEL
        # df_categoria.to_excel("df_categoria.xlsx", index=False)

        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")

    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")

    # Criar tabela de estoque geral consolidado por SKU
    print("Preparando tabela de estoque geral consolidado...")

    # Criar tabela de estoque geral consolidado por SKU
    print("Preparando tabela de estoque geral consolidado...")

    # 1. Primeiro, obter o estoque mais recente para cada combinação de produto e loja
    estoque_mais_recente = df_estoque.sort_values('data_estoque', ascending=False)
    estoque_mais_recente = estoque_mais_recente.drop_duplicates(subset=['id_produto', 'id_loja'], keep='first')

    # 2. Agora agrupar por produto, somando os valores únicos (mais recentes) de cada loja
    estoque_consolidado = estoque_mais_recente.groupby('id_produto').agg({
        'estoque': 'sum',                # Soma do estoque mais recente de todas as lojas
        'data_estoque': 'max',           # Data mais recente de atualização
        'id_loja': 'count'               # Quantidade de lojas com este produto
    }).reset_index()

    #Adicionar informações do produto
    estoque_final = pd.merge(
        estoque_consolidado,
        df_produtos[['id_produto', 'nome', 'id_categoria', 'data_criacao', 'codigo_barras']],
        on='id_produto',
        how='left'
    )
    #Adicionar informações da categoria
    estoque_final = pd.merge(
        estoque_final,
        df_categoria[['id_categoria', 'nome_categoria']],
        on='id_categoria',
        how='left'
    )

    # 3. Renomear colunas para melhor compreensão
    estoque_final = estoque_final.rename(columns={
        'id_produto': 'id_sku',
        'nome': 'nome_produto',
        'estoque': 'estoque_atual',
        'id_loja': 'qtd_lojas_com_produto',
    })

    # 4. Reorganizar colunas
    estoque_final = estoque_final[[
        'id_sku', 
        'id_categoria',
        'codigo_barras',
        'nome_produto',
        'nome_categoria',
        'data_criacao',
        'estoque_atual', 
        'data_estoque', 
        'qtd_lojas_com_produto'
    ]]

    #remover produtos com 'TAXA' na descrição
    total_antes = len(estoque_final)
    estoque_final = estoque_final[~estoque_final['nome_produto'].str.contains('TAXA', case=False, na=False)]
    total_removidos = total_antes - len(estoque_final)
    print(f"\nForam excluídos {total_removidos} produtos contendo 'TAXA' na descrição.")

    # 5. Ordenar do menor para o maior estoque para destacar produtos críticos
    estoque_final = estoque_final.sort_values('estoque_atual')

    # 6. Exibir informações da tabela
    print(f"\nTabela de Estoque Consolidado por Produto - {len(estoque_final)} produtos")
    print(estoque_final.head(10))

    # 7. Salvar em Excel
    # caminho_arquivo = os.path.join(os.path.dirname(os.path.abspath(__file__)), "estoque_geral_consolidado.xlsx")
    # estoque_final.to_excel(caminho_arquivo, index=False)
    # print(f"\nTabela exportada para: {caminho_arquivo}")

    # 8. Análise do status de estoque
    print("\n=== RESUMO DA SITUAÇÃO DE ESTOQUE ===")

    # Contagem de SKUs por status de estoque
    estoque_negativo = (estoque_final['estoque_atual'] < 0).sum()
    estoque_zerado = (estoque_final['estoque_atual'] == 0).sum()
    estoque_positivo = (estoque_final['estoque_atual'] > 0).sum()

    # Percentuais
    total_skus = len(estoque_final)
    pct_negativo = (estoque_negativo / total_skus) * 100
    pct_zerado = (estoque_zerado / total_skus) * 100
    pct_positivo = (estoque_positivo / total_skus) * 100

    # Imprimir resultados
    print(f"Total de SKUs: {total_skus}")
    print(f"- SKUs com estoque negativo: {estoque_negativo} ({pct_negativo:.1f}%)")
    print(f"- SKUs com estoque zerado: {estoque_zerado} ({pct_zerado:.1f}%)")
    print(f"- SKUs com estoque positivo: {estoque_positivo} ({pct_positivo:.1f}%)")


    # 9. Análise de vendas por período (30, 60, 90 dias e 1 ano)
    print("\n=== ANÁLISE DE VENDAS POR PERÍODO ===")

    # Preparar os dados de vendas
    print("Preparando análise de vendas por SKU em diferentes períodos...")

    # Converter coluna de data para datetime se ainda não estiver
    if not pd.api.types.is_datetime64_any_dtype(df_vendas['data_venda']):
        df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])

    #filtrar df_vendas para somente vendas que possuem a tipo_venda como PEDIDO e situacao_venda como CONCLUIDA
    df_vendas = df_vendas[(df_vendas['tipo_venda'] == 'PEDIDO') & (df_vendas['situacao_venda'] == 'CONCLUIDA')]

    # Juntar as tabelas de vendas e itens de venda
    df_venda_itens_pedido = pd.merge(
        df_venda_itens,
        df_vendas[['id_venda', 'data_venda']],
        on='id_venda',
        how='left'
    )

    # Data atual para calcular os períodos
    data_atual = datetime.now()

    # Definir os períodos de análise
    periodos = {
        'ultimos_30_dias': data_atual - timedelta(days=30),
        'ultimos_60_dias': data_atual - timedelta(days=60),
        'ultimos_90_dias': data_atual - timedelta(days=90),
        'ultimo_ano': data_atual - timedelta(days=365)
    }

    # Criar DataFrames para cada período
    vendas_por_periodo = {}
    valores_por_periodo = {}  # Novo dicionário para armazenar valores vendidos

    for periodo_nome, data_inicio in periodos.items():
        # Filtrar vendas do período
        vendas_periodo = df_venda_itens_pedido[df_venda_itens_pedido['data_venda'] >= data_inicio]
        
        # Agrupar por produto e somar quantidades
        vendas_agrupadas = vendas_periodo.groupby('id_produto')['quantidade'].sum().reset_index()
        vendas_agrupadas.rename(columns={'quantidade': f'qt_vendas_{periodo_nome}'}, inplace=True)
        
        # Agrupar por produto e somar valores vendidos
        valores_agrupados = vendas_periodo.groupby('id_produto')['total_item'].sum().reset_index()
        valores_agrupados.rename(columns={'total_item': f'valor_vendas_{periodo_nome}'}, inplace=True)
        
        # Armazenar nos dicionários
        vendas_por_periodo[periodo_nome] = vendas_agrupadas
        valores_por_periodo[periodo_nome] = valores_agrupados

    # Adicionar as vendas por período ao DataFrame de estoque
    estoque_com_vendas = estoque_final.copy()

    # Mesclar com as vendas e valores de cada período
    for periodo_nome, df_vendas_periodo in vendas_por_periodo.items():
        estoque_com_vendas = pd.merge(
            estoque_com_vendas,
            df_vendas_periodo,
            left_on='id_sku',
            right_on='id_produto',
            how='left'
        )
        
        # Mesclar com os valores vendidos
        estoque_com_vendas = pd.merge(
            estoque_com_vendas,
            valores_por_periodo[periodo_nome],
            left_on='id_sku',
            right_on='id_produto',
            how='left'
        )
        
        # Remover colunas id_produto redundantes
        if 'id_produto_x' in estoque_com_vendas.columns:
            estoque_com_vendas.drop('id_produto_x', axis=1, inplace=True)
        if 'id_produto_y' in estoque_com_vendas.columns:
            estoque_com_vendas.drop('id_produto_y', axis=1, inplace=True)
        if 'id_produto' in estoque_com_vendas.columns:
            estoque_com_vendas.drop('id_produto', axis=1, inplace=True)

    # Preencher valores nulos com zero (produtos sem vendas no período)
    for periodo_nome in periodos.keys():
        estoque_com_vendas[f'qt_vendas_{periodo_nome}'] = estoque_com_vendas[f'qt_vendas_{periodo_nome}'].fillna(0).astype(int)
        estoque_com_vendas[f'valor_vendas_{periodo_nome}'] = estoque_com_vendas[f'valor_vendas_{periodo_nome}'].fillna(0).astype(float)

    # 10. Identificar produtos mais vendidos em cada período
    print("\nTop 5 produtos mais vendidos por período:")
    for periodo_nome in periodos.keys():
        coluna_vendas = f'qt_vendas_{periodo_nome}'
        top_produtos = estoque_com_vendas.nlargest(5, coluna_vendas)
        print(f"\n{periodo_nome.replace('_', ' ').title()}:")
        for idx, row in top_produtos.iterrows():
            print(f"- SKU {int(row['id_sku'])}: {row['nome_produto'][:50]} - {int(row[coluna_vendas]):,} unidades")

    # Mostrar resumo dos valores vendidos nos últimos 30 dias
    print("\nResumo dos valores vendidos nos últimos 30 dias:")
    valor_total_30dias = estoque_com_vendas['valor_vendas_ultimos_30_dias'].sum()
    produtos_com_vendas_30dias = (estoque_com_vendas['qt_vendas_ultimos_30_dias'] > 0).sum()
    print(f"- Valor total vendido: R$ {valor_total_30dias:,.2f}")
    print(f"- Produtos vendidos: {produtos_com_vendas_30dias} SKUs")
    print(f"- Ticket médio por produto: R$ {(valor_total_30dias/produtos_com_vendas_30dias if produtos_com_vendas_30dias > 0 else 0):,.2f}")


    # 11. Adicionar situação do produto
    print("\nClassificando produtos por situação...")
    # Preencher valores nulos com zero (produtos sem vendas no período)
    for periodo_nome in periodos.keys():
        estoque_com_vendas[f'qt_vendas_{periodo_nome}'] = estoque_com_vendas[f'qt_vendas_{periodo_nome}'].fillna(0).astype(int)

    # Consulta SQL para buscar a data da última venda de cada produto
    query_ultima_venda = f"""
    SELECT vi.id_produto, MAX(v.data_venda) as ultima_venda
    FROM {schema}.venda_item vi
    JOIN {schema}.venda v ON vi.id_venda = v.id_venda
    GROUP BY vi.id_produto
    """

    # Consulta SQL para buscar a primeira data de venda de cada produto (histórico anterior a 1 ano)
    query_vendas_historicas = f"""
    SELECT vi.id_produto, MIN(v.data_venda) as primeira_venda
    FROM {schema}.venda_item vi
    JOIN {schema}.venda v ON vi.id_venda = v.id_venda
    WHERE v.data_venda < %s
    GROUP BY vi.id_produto
    """


    # Executar consulta para obter produtos com vendas anteriores a 1 ano
    conn = psycopg2.connect(
        host=DB_CONFIG_MALOKA['host'],
        database=database,
        user=DB_CONFIG_MALOKA['user'],
        password=DB_CONFIG_MALOKA['password'],
        port=DB_CONFIG_MALOKA['port']
    )
    cursor = conn.cursor()

    # Buscar a data da última venda de cada produto
    print("Consultando a data da última venda de cada produto...")
    cursor.execute(query_ultima_venda)
    ultimas_vendas = {row[0]: row[1] for row in cursor.fetchall()}

    # Buscar dados para classificação de produtos com histórico antigo
    data_um_ano_atras = data_atual - timedelta(days=365)
    cursor.execute(query_vendas_historicas, (data_um_ano_atras,))
    vendas_historicas = {row[0]: row[1] for row in cursor.fetchall()}

    conn.close()

    # Adicionar coluna de vendas históricas ao DataFrame principal
    estoque_com_vendas['tem_vendas_mais_que_1_ano'] = estoque_com_vendas['id_sku'].apply(
        lambda sku: "Sim" if sku in vendas_historicas else "Não"
    )

    # Adicionar coluna com a data da última venda
    estoque_com_vendas['data_ultima_venda'] = estoque_com_vendas['id_sku'].apply(
        lambda sku: ultimas_vendas.get(sku, None)
    )

    # Calcular recência (dias desde a última venda)
    estoque_com_vendas['recencia_dias'] = estoque_com_vendas['data_ultima_venda'].apply(
        lambda data: (data_atual - data).days if pd.notnull(data) else None
    )

    # Resumo de recência
    produtos_com_venda = estoque_com_vendas['recencia_dias'].notna().sum()
    print(f"\nProdutos com histórico de vendas: {produtos_com_venda} ({produtos_com_venda/total_skus*100:.1f}% do total)")

    # Criar função para classificar situação
    def classificar_situacao_produto(row):
        # Verificar se teve venda no último ano
        sem_venda_ano = row['qt_vendas_ultimo_ano'] == 0
        
        # Verificar se tem vendas históricas (mais de um ano)
        tem_vendas_historicas = row['tem_vendas_mais_que_1_ano'] == "Sim"
        
        # Se não teve venda no último ano, mas teve anteriormente
        if sem_venda_ano:
            if tem_vendas_historicas:
                if row['estoque_atual'] > 0:
                    return "Inativo (ESTOQUE > 0)"
                else:
                    return "Inativo (ESTOQUE <= 0)"
            else:
                # Se nunca teve venda (novo produto)
                if row['estoque_atual'] > 0:
                    return "Não Comercializado (ESTOQUE > 0)"
                else:
                    return "Não Comercializado (ESTOQUE <= 0)"
        else:
            # Teve vendas no último ano
            if row['estoque_atual'] > 0:
                return "Ativo (ESTOQUE > 0)"
            else:
                return "Ativo (ESTOQUE <= 0)"

    # Aplicar a classificação
    estoque_com_vendas['situacao_do_produto'] = estoque_com_vendas.apply(classificar_situacao_produto, axis=1)

    # Resumo da situação dos produtos
    situacao_counts = estoque_com_vendas['situacao_do_produto'].value_counts()
    print("\n=== SITUAÇÃO DOS PRODUTOS ===")
    for situacao, count in situacao_counts.items():
        percentual = (count / total_skus) * 100
        print(f"- {situacao}: {count:,} produtos ({percentual:.1f}%)")

    # Resumo de vendas
    print("\nResumo de vendas por período:")
    for periodo_nome in periodos.keys():
        coluna_vendas = f'qt_vendas_{periodo_nome}'
        coluna_valores = f'valor_vendas_{periodo_nome}'
        total_vendas = estoque_com_vendas[coluna_vendas].sum()
        total_valores = estoque_com_vendas[coluna_valores].sum()
        produtos_vendidos = (estoque_com_vendas[coluna_vendas] > 0).sum()
        print(f"- {periodo_nome.replace('_', ' ').title()}: {total_vendas:,} unidades vendidas em {produtos_vendidos:,} SKUs, valor total: R$ {total_valores:,.2f}")


    # 12. Análise de Curva ABC para produtos vendidos nos últimos 90 dias
    print("\n=== ANÁLISE DE CURVA ABC (ÚLTIMOS 90 DIAS) ===")
    # Filtrar apenas os produtos com vendas nos últimos 90 dias
    produtos_com_vendas = estoque_com_vendas[estoque_com_vendas['qt_vendas_ultimos_90_dias'] > 0].copy()

    if len(produtos_com_vendas) > 0:
        # Calcular a participação de cada produto no valor total das vendas
        valor_total = produtos_com_vendas['valor_vendas_ultimos_90_dias'].sum()
        produtos_com_vendas['participacao'] = produtos_com_vendas['valor_vendas_ultimos_90_dias'] / valor_total * 100
        
        # Ordenar por valor de venda decrescente
        produtos_com_vendas = produtos_com_vendas.sort_values('valor_vendas_ultimos_90_dias', ascending=False)
        
        # Calcular a participação acumulada
        produtos_com_vendas['participacao_acumulada'] = produtos_com_vendas['participacao'].cumsum()
        
        # Definir as classes ABC
        produtos_com_vendas['curva_abc'] = 'C'
        produtos_com_vendas.loc[produtos_com_vendas['participacao_acumulada'] <= 80, 'curva_abc'] = 'A'
        produtos_com_vendas.loc[(produtos_com_vendas['participacao_acumulada'] > 80) & 
                            (produtos_com_vendas['participacao_acumulada'] <= 95), 'curva_abc'] = 'B'
        
        # Adicionar a classificação ao DataFrame original
        estoque_com_vendas['curva_abc'] = None
        for idx, row in produtos_com_vendas.iterrows():
            estoque_com_vendas.loc[estoque_com_vendas['id_sku'] == row['id_sku'], 'curva_abc'] = row['curva_abc']
        
        # Substituir None por 'Sem Venda' para produtos sem vendas nos últimos 90 dias
        estoque_com_vendas['curva_abc'] = estoque_com_vendas['curva_abc'].fillna('Sem Venda')
        
        # Resumo detalhado da classificação ABC
        curva_counts = produtos_com_vendas['curva_abc'].value_counts().sort_index()
        total_produtos_com_vendas = len(produtos_com_vendas)
        
        print("\n=== DETALHAMENTO DA CURVA ABC ===")
        print(f"Total de produtos com vendas nos últimos 90 dias: {total_produtos_com_vendas}")
        
        # Calcular totais por curva
        for curva in ['A', 'B', 'C']:
            if curva in curva_counts:
                count = curva_counts[curva]
                pct_produtos = (count / total_produtos_com_vendas) * 100
                
                # Calcular valor total para esta curva
                valor_curva = produtos_com_vendas[produtos_com_vendas['curva_abc'] == curva]['valor_vendas_ultimos_90_dias'].sum()
                pct_valor = (valor_curva / valor_total) * 100
                
                # Calcular estoque médio para esta curva
                estoque_medio = produtos_com_vendas[produtos_com_vendas['curva_abc'] == curva]['estoque_atual'].mean()
                
                print(f"\nCurva {curva}:")
                print(f"- Quantidade de produtos: {count} ({pct_produtos:.1f}% dos produtos com vendas)")
                print(f"- Valor total vendido: R$ {valor_curva:,.2f} ({pct_valor:.1f}% do faturamento)")
                print(f"- Estoque médio atual: {estoque_medio:.1f} unidades por produto")
                
                # Adicionar informação de cobertura de estoque (dias)
                vendas_diarias = produtos_com_vendas[produtos_com_vendas['curva_abc'] == curva]['qt_vendas_ultimos_90_dias'].sum() / 90
                estoque_total_curva = produtos_com_vendas[produtos_com_vendas['curva_abc'] == curva]['estoque_atual'].sum()
                if vendas_diarias > 0:
                    cobertura = estoque_total_curva / vendas_diarias
                    print(f"- Cobertura média de estoque: {cobertura:.1f} dias")
        
        # Análise de valor por curva
        print("\nDistribuição do valor vendido por Curva ABC (últimos 90 dias):")
        curva_valores = estoque_com_vendas.groupby('curva_abc')['valor_vendas_ultimos_90_dias'].sum()
        for curva, valor in curva_valores.items():
            if valor_total > 0:
                percentual = (valor / valor_total) * 100
                print(f"- Curva {curva}: R$ {valor:,.2f} ({percentual:.1f}%)")
            else:
                print(f"- Curva {curva}: R$ {valor:,.2f} (0.0%)")

    else:
        print("Não foram encontrados produtos com vendas nos últimos 90 dias.")
        estoque_com_vendas['curva_abc'] = 'Sem Venda'

    # 13. Análise de cobertura de estoque para todos os produtos por curva ABC
    print("\n=== ANÁLISE DE COBERTURA DE ESTOQUE POR CURVA ABC ===")

    # Utilizando todos os produtos, sem filtro de ativos
    produtos_abc = estoque_com_vendas[estoque_com_vendas['curva_abc'].isin(['A', 'B', 'C'])].copy()
    print(f"Total de produtos na Curva ABC analisados: {len(produtos_abc)}")

    # Calcular a cobertura de estoque (em dias) com base nas vendas dos últimos 90 dias
    produtos_abc['vendas_diarias'] = produtos_abc['qt_vendas_ultimos_90_dias'] / 90
    produtos_abc['cobertura_estoque_dias'] = 0  # Valor padrão

    # Evitar divisão por zero e definir cobertura zero para estoque zero ou negativo
    mask = (produtos_abc['vendas_diarias'] > 0) & (produtos_abc['estoque_atual'] > 0)
    produtos_abc.loc[mask, 'cobertura_estoque_dias'] = (
        produtos_abc.loc[mask, 'estoque_atual'] / produtos_abc.loc[mask, 'vendas_diarias']
    )

    # Copiar os valores calculados para o DataFrame principal
    estoque_com_vendas['vendas_por_dia'] = 0
    estoque_com_vendas['cobertura_estoque_em_dias'] = 0

    for idx, row in produtos_abc.iterrows():
        estoque_com_vendas.loc[estoque_com_vendas['id_sku'] == row['id_sku'], 'vendas_por_dia'] = row['vendas_diarias']
        estoque_com_vendas.loc[estoque_com_vendas['id_sku'] == row['id_sku'], 'cobertura_estoque_em_dias'] = row['cobertura_estoque_dias']

    # Estatísticas da cobertura de estoque por curva
    print("\nEstatísticas de cobertura de estoque por curva ABC (em dias):")
    for curva in ['A', 'B', 'C']:
        produtos_curva = produtos_abc[produtos_abc['curva_abc'] == curva]
        if len(produtos_curva) > 0:
            cobertura_stats = produtos_curva['cobertura_estoque_dias'].describe()
            print(f"\nCurva {curva} ({len(produtos_curva)} produtos):")
            print(f"- Média: {cobertura_stats['mean']:.1f}")
            print(f"- Mediana: {cobertura_stats['50%']:.1f}")
            print(f"- Mínimo: {cobertura_stats['min']:.1f}")
            print(f"- Máximo: {cobertura_stats['max']:.1f}")
        else:
            print(f"\nCurva {curva}: Nenhum produto encontrado")

    # Distribuição da cobertura de estoque
    print("\nDistribuição da cobertura de estoque por curva ABC:")
    bins = [-1, 0, 15, 30, 60, 90, 180, float('inf')]
    labels = ['0 dias', '1-15 dias', '16-30 dias', '31-60 dias', '61-90 dias', '91-180 dias', 'Mais de 180 dias']

    for curva in ['A', 'B', 'C']:
        print(f"\nCurva {curva}:")
        produtos_curva = produtos_abc[produtos_abc['curva_abc'] == curva]
        if len(produtos_curva) > 0:
            produtos_curva['faixa_cobertura'] = pd.cut(produtos_curva['cobertura_estoque_dias'], bins=bins, labels=labels)
            cobertura_dist = produtos_curva['faixa_cobertura'].value_counts().sort_index()
            
            for faixa, count in cobertura_dist.items():
                pct = (count / len(produtos_curva)) * 100
                print(f"- {faixa}: {count} produtos ({pct:.1f}%)")
        else:
            print("  Nenhum produto encontrado")

    #############################################################
    ### Exportar o DataFrame completo e métricas para análise ###
    #############################################################

    print("Consultando a tabela COMPRA_ITEM para obter o último preço de compra...")
    query = f"""
    WITH RankedPurchases AS (
        SELECT 
            ci.id_produto,
            ci.preco_bruto,
            c.data_compra,
            ROW_NUMBER() OVER(PARTITION BY ci.id_produto ORDER BY c.data_compra DESC) as rn
        FROM 
            {schema}.compra_item ci
        JOIN 
            {schema}.compra c ON ci.id_compra = c.id_compra
    )
    SELECT 
        id_produto, 
        preco_bruto as ultimo_preco_compra
    FROM 
        RankedPurchases
    WHERE 
        rn = 1
    """

    # Carregar os dados diretamente em um DataFrame do pandas
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG_MALOKA['host'],
            database=database,
            user=DB_CONFIG_MALOKA['user'],
            password=DB_CONFIG_MALOKA['password'],
            port=DB_CONFIG_MALOKA['port']
        )
        df_precos_compra = pd.read_sql_query(query, conn)
        conn.close()
        
        # Informações sobre os dados obtidos
        num_registros = len(df_precos_compra)
        print(f"Dados de preços de compra obtidos com sucesso! {num_registros} SKUs com último preço de compra.")
        
        # Mesclar com o DataFrame de estoque
        estoque_com_vendas = pd.merge(
            estoque_com_vendas,
            df_precos_compra,
            left_on='id_sku',
            right_on='id_produto',
            how='left'
        )
        
        # Remover coluna id_produto redundante, se existir
        if 'id_produto' in estoque_com_vendas.columns:
            estoque_com_vendas.drop('id_produto', axis=1, inplace=True)
        
        # Tratar valores nulos no preço de compra
        # Para SKUs sem preço de compra registrado, usar um valor padrão ou 0
        estoque_com_vendas['ultimo_preco_compra'].fillna(0, inplace=True)
        
        # Calcular o valor de estoque em custo de compra usando o último preço
        estoque_com_vendas['valor_estoque_custo'] = estoque_com_vendas['estoque_atual'] * estoque_com_vendas['ultimo_preco_compra']
        
    except Exception as e:
        print(f"Erro ao consultar preços de compra: {e}")
        print("Utilizando quantidade como proxy para custo.")
        # Fallback: criar coluna de valor baseado apenas na quantidade
        estoque_com_vendas['ultimo_preco_compra'] = 1
        estoque_com_vendas['valor_estoque_custo'] = estoque_com_vendas['estoque_atual']

    # Criar dataframe com as métricas solicitadas em uma única linha
    metricas = {}

    # DATA_HORA_ANALISE
    metricas['data_hora_analise'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Total de SKUs 
    total_skus = len(estoque_com_vendas)
    metricas['total_de_skus'] = total_skus

    # Total e percentual de SKUs com venda acima de 1 ano
    total_sku_venda_acima_1ano = (estoque_com_vendas['tem_vendas_mais_que_1_ano'] == "Sim").sum()
    metricas['total_sku_com_mais_de_um_ano_de_vendas'] = total_sku_venda_acima_1ano
    metricas['percent_sku_com_mais_de_um_ano_de_vendas'] = (total_sku_venda_acima_1ano / total_skus) * 100

    # Total e percentual de SKUs com venda somente no último ano
    total_sku_venda_ultimo_ano = ((estoque_com_vendas['qt_vendas_ultimo_ano'] > 0) & (estoque_com_vendas['tem_vendas_mais_que_1_ano'] == "Não")).sum()
    metricas['total_sku_com_ate_um_ano_de_vendas'] = total_sku_venda_ultimo_ano
    metricas['percent_sku_com_ate_um_ano_de_vendas'] = (total_sku_venda_ultimo_ano / total_skus) * 100

    # Total e percentual de SKUs com estoque zero
    total_sku_estoque_zero = (estoque_com_vendas['estoque_atual'] == 0).sum()
    metricas['total_sku_com_estoque_zero'] = total_sku_estoque_zero
    metricas['percent_sku_com_estoque_zero'] = (total_sku_estoque_zero / total_skus) * 100

    # Total e percentual de SKUs com estoque positivo
    total_sku_estoque_positivo = (estoque_com_vendas['estoque_atual'] > 0).sum()
    metricas['total_sku_com_estoque_positivo'] = total_sku_estoque_positivo
    metricas['percent_sku_com_estoque_positivo'] = (total_sku_estoque_positivo / total_skus) * 100

    # Custo total de estoque positivo (vamos usar a quantidade como proxy para o custo, já que não temos o valor unitário)
    metricas['custo_total_estoque_positivo'] = estoque_com_vendas[estoque_com_vendas['estoque_atual'] > 0]['valor_estoque_custo'].sum()

    # Total e percentual de SKUs com estoque negativo
    total_sku_estoque_negativo = (estoque_com_vendas['estoque_atual'] < 0).sum()
    metricas['total_sku_com_estoque_negativo'] = total_sku_estoque_negativo
    metricas['percent_sku_com_estoque_negativo'] = (total_sku_estoque_negativo / total_skus) * 100

    # Custo total de estoque negativo
    metricas['custo_total_estoque_negativo'] = abs(estoque_com_vendas[estoque_com_vendas['estoque_atual'] < 0]['valor_estoque_custo'].sum())

    # Total e percentual de SKUs inativos com saldo
    total_inativo_com_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Inativo (ESTOQUE > 0)').sum()
    metricas['total_sku_inativo_com_estoque_maior_que_zero'] = total_inativo_com_saldo
    metricas['percent_sku_inativo_com_estoque_maior_que_zero'] = (total_inativo_com_saldo / total_skus) * 100

    # Custo total de inativos com saldo
    filtro_inativo_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Inativo (ESTOQUE > 0)')
    metricas['custo_total_inativo_com_estoque_maior_que_zero'] = estoque_com_vendas[filtro_inativo_saldo]['valor_estoque_custo'].sum()

    # Total e percentual de SKUs inativos sem saldo
    total_inativo_sem_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Inativo (ESTOQUE <= 0)').sum()
    metricas['total_sku_inativo_com_estoque_menor_ou_igual_zero'] = total_inativo_sem_saldo
    metricas['percent_sku_inativo_com_estoque_menor_ou_igual_zero'] = (total_inativo_sem_saldo / total_skus) * 100

    # Total e percentual de SKUs ativos com saldo
    total_ativo_com_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Ativo (ESTOQUE > 0)').sum()
    metricas['total_sku_ativo_com_estoque_maior_que_zero'] = total_ativo_com_saldo
    metricas['percent_sku_ativo_com_estoque_maior_que_zero'] = (total_ativo_com_saldo / total_skus) * 100

    # Custo total de ativos com saldo
    metricas['custo_total_ativo_com_estoque_maior_que_zero'] = estoque_com_vendas[estoque_com_vendas['situacao_do_produto'] == 'Ativo (ESTOQUE > 0)']['valor_estoque_custo'].sum()

    # Total e percentual de SKUs ativos sem saldo
    total_ativo_sem_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Ativo (ESTOQUE <= 0)').sum()
    metricas['total_sku_ativo_com_estoque_menor_ou_igual_zero'] = total_ativo_sem_saldo
    metricas['percent_sku_ativo_com_estoque_menor_ou_igual_zero'] = (total_ativo_sem_saldo / total_skus) * 100

    # Total e percentual de SKUs sem venda com saldo
    filtro_sem_venda_com_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Não Comercializado (ESTOQUE > 0)')
    total_sem_venda_com_saldo = filtro_sem_venda_com_saldo.sum()
    metricas['total_sku_nao_comercializado_com_estoque_maior_que_zero'] = total_sem_venda_com_saldo
    metricas['percent_sku_nao_comercializado_com_estoque_maior_que_zero'] = (total_sem_venda_com_saldo / total_skus) * 100

    # Custo total de sem venda com saldo
    metricas['custo_total_nao_comercializado_com_estoque_maior_que_zero'] = estoque_com_vendas[filtro_sem_venda_com_saldo]['valor_estoque_custo'].sum()

    # Total e percentual de SKUs sem venda sem saldo
    total_sem_venda_sem_saldo = (estoque_com_vendas['situacao_do_produto'] == 'Não Comercializado (ESTOQUE <= 0)').sum()
    metricas['total_sku_nao_comercializado_com_estoque_menor_ou_igual_zero'] = total_sem_venda_sem_saldo
    metricas['percent_sku_nao_comercializado_com_estoque_menor_ou_igual_zero'] = (total_sem_venda_sem_saldo / total_skus) * 100

    # Análise de consistência de movimentações de estoque usando DataFrames já carregados
    print("\n=== ANÁLISE DE CONSISTÊNCIA DE MOVIMENTAÇÕES DE ESTOQUE ===")
    try:
        # Preparar o DataFrame com o histórico de estoque mais recente por produto/loja
        print("Analisando consistência entre histórico de estoque e últimas movimentações...")
        
        # Obter o estoque mais recente para cada par produto/loja do histórico
        historico_mais_recente = df_estoque.sort_values(['id_produto', 'id_loja', 'data_estoque'], ascending=[True, True, False])
        historico_mais_recente = historico_mais_recente.drop_duplicates(subset=['id_produto', 'id_loja'], keep='first')
        
        # Obter a última movimentação para cada par produto/loja
        # Ordenando por data de movimento (mais recente primeiro) e ordem de movimento
        movimento_mais_recente = df_estoque_movi.sort_values(
            ['id_produto', 'id_loja', 'data_movimento', 'ordem_movimento'], 
            ascending=[True, True, False, False]
        )
        movimento_mais_recente = movimento_mais_recente.drop_duplicates(subset=['id_produto', 'id_loja'], keep='first')
        
        # Juntar os dois DataFrames para comparação
        analise_consistencia = pd.merge(
            historico_mais_recente[['id_produto', 'id_loja', 'estoque', 'data_estoque']],
            movimento_mais_recente[['id_produto', 'id_loja', 'estoque_depois', 'data_movimento']],
            on=['id_produto', 'id_loja'],
            how='left'
        )
        
        # Calcular a diferença e determinar se é consistente
        # Definindo um limite de tolerância (0.01) para considerar diferenças de arredondamento
        analise_consistencia['estoque_depois'] = analise_consistencia['estoque_depois'].fillna(0)
        analise_consistencia['diferenca'] = abs(analise_consistencia['estoque'] - analise_consistencia['estoque_depois'])
        analise_consistencia['status_consistencia'] = analise_consistencia['diferenca'].apply(
            lambda x: 'Consistente' if x <= 0.01 else 'Inconsistente'
        )
        
        # Total de produtos verificados
        total_skus_verificados = analise_consistencia['id_produto'].nunique()
        
        # Contagem de produtos consistentes e inconsistentes
        consistentes = analise_consistencia[analise_consistencia['status_consistencia'] == 'Consistente']['id_produto'].nunique()
        inconsistentes = total_skus_verificados - consistentes
        
        # Calcular percentuais
        pct_consistentes = (consistentes / total_skus_verificados * 100) if total_skus_verificados > 0 else 0
        pct_inconsistentes = 100 - pct_consistentes
        
        # Adicionar às métricas
        metricas['total_sku_verificados'] = total_skus_verificados
        metricas['total_sku_consistentes'] = consistentes
        metricas['percent_sku_consistentes'] = pct_consistentes
        metricas['total_sku_inconsistentes'] = inconsistentes
        metricas['percent_sku_inconsistentes'] = pct_inconsistentes
        
        # Estatísticas adicionais sobre as inconsistências
        inconsistencias = analise_consistencia[analise_consistencia['status_consistencia'] == 'Inconsistente']
        
        if len(inconsistencias) > 0:
            # Exibir top 10 inconsistências
            print(f"TOTAL de SKU verificados: {total_skus_verificados}")
            print(f"SKU consistentes: {consistentes} ({pct_consistentes:.1f}%)")
            print(f"SKU inconsistentes: {inconsistentes} ({pct_inconsistentes:.1f}%)")
            
            # Exibir as 5 maiores inconsistências 
            top_inconsistentes = inconsistencias.sort_values('diferenca', ascending=False).head(5)
            if len(top_inconsistentes) > 0:
                print("\nAs 5 maiores inconsistências encontradas:")
                for _, row in top_inconsistentes.iterrows():
                    print(f"- Produto ID {int(row['id_produto'])}, Loja {int(row['id_loja'])}: " 
                        f"Saldo histórico = {row['estoque']:.1f}, "
                        f"Saldo movimentação = {row['estoque_depois']:.1f}, "
                        f"Diferença = {row['diferenca']:.1f}")
            
            #renoemar nomes dos dois estoques
            analise_consistencia.rename(columns={
                'estoque': 'estoque_historico',
                'estoque_depois': 'estoque_movimentacao',
                'data_estoque': 'data_estoque_historico',
                'data_movimento': 'data_estoque_movimentacao'
            }, inplace=True)

            ###############################
            # Salvar relatório
            ###############################
            # Exportar detalhes das inconsistências
            # csv_path = os.path.join(diretorio_cliente, f'{nome_cliente}_analise_consistencia_estoque.csv')
            # analise_consistencia.to_csv(csv_path, index=False)
            print(f"Detalhes de inconsistências gerado para {nome_cliente}")
        else:
            analise_consistencia.rename(columns={
                'estoque': 'estoque_historico',
                'estoque_depois': 'estoque_movimentacao',
                'data_estoque': 'data_estoque_historico',
                'data_movimento': 'data_estoque_movimentacao'
            }, inplace=True)
            print("\nNão foram encontradas inconsistências entre o histórico de estoque e os movimentos.")
            
            ###############################
            # Salvar relatório
            ###############################
            # csv_path = os.path.join(diretorio_cliente, f'{nome_cliente}_analise_consistencia_estoque.csv')
            # analise_consistencia.to_csv(csv_path, index=False)
            print(f"Detalhes de inconsistências gerado para {nome_cliente}")
        
        # # Exportar resultados de inconsistências para o banco de dados no esquema maloka_analytics
        # print("Exportando resultados de inconsistências para o banco de dados...")
        
        # try:
        #     # Reconectar ao PostgreSQL
        #     print("Conectando ao banco de dados PostgreSQL...")
        #     conn = psycopg2.connect(
            #     host= DB_HOST,
            #     database=database,
            #     user= DB_USER,
            #     password= DB_PASS,
            #     port= DB_PORT
            # )
            
        #     # Criar cursor
        #     cursor = conn.cursor()
            
        #     # Verificar se o esquema maloka_analytics existe, caso contrário, criar
        #     cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'maloka_analytics')")
        #     schema_existe = cursor.fetchone()[0]
            
        #     if not schema_existe:
        #         print(f"Esquema maloka_analytics não existe no banco {database}. Criando...")
        #         cursor.execute("CREATE SCHEMA maloka_analytics")
        #     else: 
        #         print(f"Esquema maloka_analytics já existe no banco {database}.")
            
        #     # Verificar se a tabela já existe no esquema maloka_analytics
        #     cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='consistencia_estoque' AND table_schema='maloka_analytics')")
        #     tabela_existe = cursor.fetchone()[0]
            
        #     if tabela_existe:
        #         # Truncar a tabela se ela já existir
        #         print("Tabela consistencia_estoque já existe no esquema maloka_analytics. Limpando dados existentes...")
        #         cursor.execute("TRUNCATE TABLE maloka_analytics.consistencia_estoque")
        #     else:
        #         # Criar a tabela se não existir
        #         print("Criando tabela consistencia_estoque no esquema maloka_analytics...")
        #         # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
        #         colunas = []
        #         for coluna, dtype in analise_consistencia.dtypes.items():
        #             if 'int' in str(dtype):
        #                 tipo = 'INTEGER'
        #             elif 'float' in str(dtype):
        #                 tipo = 'DECIMAL'
        #             elif 'datetime' in str(dtype):
        #                 tipo = 'TIMESTAMP'
        #             else:
        #                 tipo = 'TEXT'
        #             colunas.append(f'"{coluna}" {tipo}')
                
        #         create_table_query = f"""
        #         CREATE TABLE maloka_analytics.consistencia_estoque (
        #             {", ".join(colunas)}
        #         )
        #         """
        #         cursor.execute(create_table_query)
            
        #     # Otimização da inserção de dados para a tabela de análise de estoque
        #     print(f"Inserindo {len(analise_consistencia)} registros na tabela consistencia_estoque...")
            
        #     # Converter NaN para None
        #     df_upload = analise_consistencia.replace({np.nan: None})
            
        #     # Aumentar o tamanho do lote para melhor performance
        #     batch_size = 5000
            
        #     # Preparar a query uma única vez fora do loop
        #     colunas = [f'"{col}"' for col in df_upload.columns]
        #     placeholders = ", ".join(["%s"] * len(df_upload.columns))
        #     insert_query = f"""
        #     INSERT INTO maloka_analytics.consistencia_estoque ({", ".join(colunas)})
        #     VALUES ({placeholders})
        #     """
            
        #     # Criar uma lista de tuplas com os valores
        #     valores = [tuple(row) for _, row in df_upload.iterrows()]
            
        #     # Executar a inserção em lotes
        #     for i in range(0, len(valores), batch_size):
        #         batch = valores[i:i+batch_size]
        #         cursor.executemany(insert_query, batch)
        #         # Commit a cada lote para não sobrecarregar a memória
        #         conn.commit()
        #         print(f"  Inseridos {min(i+batch_size, len(valores))}/{len(valores)} registros...")
            
        #     print(f"Dados de análise de consistência de estoque inseridos com sucesso! Total de {len(df_upload)} registros.")
            
        #     # Fechar cursor e conexão
        #     cursor.close()
        #     conn.close()
            
        # except Exception as e:
        #     print(f"Erro ao inserir dados no banco: {e}")
        #     if 'conn' in locals() and conn is not None:
        #         conn.close()
        
    except Exception as e:
        print(f"Erro ao analisar consistência de estoque: {e}")
        metricas['total_sku_verificados'] = 0
        metricas['total_sku_consistentes'] = 0
        metricas['percent_sku_consistentes'] = 0
        metricas['total_sku_inconsistentes'] = 0
        metricas['percent_sku_inconsistentes'] = 0

    # Totais por grupo ABC
    total_grupo_a = (estoque_com_vendas['curva_abc'] == 'A').sum()
    total_grupo_b = (estoque_com_vendas['curva_abc'] == 'B').sum()
    total_grupo_c = (estoque_com_vendas['curva_abc'] == 'C').sum()

    # Total de produtos com vendas nos últimos 90 dias (base para os percentuais da curva ABC)
    total_produtos_com_vendas_90dias = (estoque_com_vendas['qt_vendas_ultimos_90_dias'] > 0).sum()

    metricas['total_sku_grupo_a'] = total_grupo_a
    metricas['total_sku_grupo_b'] = total_grupo_b
    metricas['total_sku_grupo_c'] = total_grupo_c

    # Percentuais por grupo ABC (baseado apenas nos produtos com vendas nos últimos 90 dias)
    metricas['percent_sku_grupo_a'] = (total_grupo_a / total_produtos_com_vendas_90dias) * 100 if total_produtos_com_vendas_90dias > 0 else 0
    metricas['percent_sku_grupo_b'] = (total_grupo_b / total_produtos_com_vendas_90dias) * 100 if total_produtos_com_vendas_90dias > 0 else 0
    metricas['percent_sku_grupo_c'] = (total_grupo_c / total_produtos_com_vendas_90dias) * 100 if total_produtos_com_vendas_90dias > 0 else 0

    # Total venda por grupo ABC (usando valor_ultimos_90_dias)
    venda_grupo_a = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'A']['valor_vendas_ultimos_90_dias'].sum()
    venda_grupo_b = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'B']['valor_vendas_ultimos_90_dias'].sum()
    venda_grupo_c = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'C']['valor_vendas_ultimos_90_dias'].sum()

    metricas['total_venda_grupo_a'] = venda_grupo_a
    metricas['total_venda_grupo_b'] = venda_grupo_b
    metricas['total_venda_grupo_c'] = venda_grupo_c

    # Percentual venda por grupo ABC
    venda_total = venda_grupo_a + venda_grupo_b + venda_grupo_c
    metricas['percent_venda_grupo_a'] = (venda_grupo_a / venda_total) * 100 if venda_total > 0 else 0
    metricas['percent_venda_grupo_b'] = (venda_grupo_b / venda_total) * 100 if venda_total > 0 else 0
    metricas['percent_venda_grupo_c'] = (venda_grupo_c / venda_total) * 100 if venda_total > 0 else 0

    # Cálculo da cobertura em dias por grupo ABC
    # Para grupo A
    estoque_grupo_a = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'A']['estoque_atual'].sum()
    vendas_diarias_grupo_a = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'A']['qt_vendas_ultimos_90_dias'].sum() / 90
    cobertura_grupo_a = estoque_grupo_a / vendas_diarias_grupo_a if vendas_diarias_grupo_a > 0 else 0
    metricas['cobertura_em_dias_grupo_a'] = cobertura_grupo_a

    # Para grupo B
    estoque_grupo_b = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'B']['estoque_atual'].sum()
    vendas_diarias_grupo_b = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'B']['qt_vendas_ultimos_90_dias'].sum() / 90
    cobertura_grupo_b = estoque_grupo_b / vendas_diarias_grupo_b if vendas_diarias_grupo_b > 0 else 0
    metricas['cobertura_em_dias_grupo_b'] = cobertura_grupo_b

    # Para grupo C
    estoque_grupo_c = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'C']['estoque_atual'].sum()
    vendas_diarias_grupo_c = estoque_com_vendas[estoque_com_vendas['curva_abc'] == 'C']['qt_vendas_ultimos_90_dias'].sum() / 90
    cobertura_grupo_c = estoque_grupo_c / vendas_diarias_grupo_c if vendas_diarias_grupo_c > 0 else 0
    metricas['cobertura_em_dias_grupo_c'] = cobertura_grupo_c

    # Criar DataFrame com as métricas
    df_metricas = pd.DataFrame([metricas])

    # Identificar todas as lojas disponíveis
    # print("\nAdicionando informações de estoque por loja e consistência...")
    # todas_lojas = df_estoque['id_loja'].unique()
    # todas_lojas = sorted(todas_lojas)  # Ordenar lojas por ID

    # # Para cada loja, adicionar uma coluna com o estoque
    # # Primeiro criar um dicionário para mapear o estoque de cada produto em cada loja
    # estoque_por_loja = {}
    # for loja in todas_lojas:
    #     # Filtrar o estoque mais recente para esta loja
    #     estoque_loja = historico_mais_recente[historico_mais_recente['id_loja'] == loja]
    #     # Criar mapeamento de produto para estoque nesta loja
    #     mapa_estoque = dict(zip(estoque_loja['id_produto'], estoque_loja['estoque']))
    #     estoque_por_loja[loja] = mapa_estoque

    # # Adicionar colunas de estoque por loja ao DataFrame principal
    # for loja in todas_lojas:
    #     coluna_estoque = f'estoque_loja_{loja}'
    #     estoque_com_vendas[coluna_estoque] = estoque_com_vendas['id_sku'].map(
    #         lambda sku: estoque_por_loja[loja].get(sku, 0)
    #     )

    # # Adicionar informação de consistência por loja
    # consistencia_por_loja = {}
    # for loja in todas_lojas:
    #     # Filtrar a análise de consistência para esta loja
    #     consistencia_loja = analise_consistencia[analise_consistencia['id_loja'] == loja]
    #     # Criar mapeamento de produto para status de consistência nesta loja
    #     mapa_consistencia = dict(zip(consistencia_loja['id_produto'], consistencia_loja['status_consistencia']))
    #     consistencia_por_loja[loja] = mapa_consistencia

    # # Adicionar colunas de consistência por loja ao DataFrame principal
    # for loja in todas_lojas:
    #     coluna_consistencia = f'consistência_loja_{loja}'
    #     estoque_com_vendas[coluna_consistencia] = estoque_com_vendas['id_sku'].map(
    #         lambda sku: consistencia_por_loja[loja].get(sku, 'Sem Informação')
    #     )

    # print(f"Adicionadas {len(todas_lojas)} colunas de estoque por loja e {len(todas_lojas)} colunas de consistência.")
    
    ###############################
    # Salvar relatório
    ###############################
    # caminho_arquivo_estoque = os.path.join(diretorio_cliente, f'{nome_cliente}_analise_estoque.csv')
    # estoque_com_vendas.to_csv(caminho_arquivo_estoque, index=False)
    print(f"\nRelatorio de estoque detalhado gerado para {nome_cliente}")

    # Exportar resultados para os esquemas maloka_analytics e maloka_core
    print("\n=== EXPORTANDO ANÁLISE DE ESTOQUE PARA O BANCO DE DADOS ===")
    try:
        # Reconectar ao PostgreSQL
        print("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_CONFIG_MALOKA['host'],
            database=database,
            user=DB_CONFIG_MALOKA['user'],
            password=DB_CONFIG_MALOKA['password'],
            port=DB_CONFIG_MALOKA['port']
        )
        
        # Criar cursor
        cursor = conn.cursor()
        
        # Verificar e criar os esquemas necessários (maloka_analytics e maloka_core)
        for schema in ['maloka_analytics', 'maloka_core']:
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema}')")
            schema_existe = cursor.fetchone()[0]
            
            if not schema_existe:
                print(f"Esquema {schema} não existe no banco {database}. Criando...")
                cursor.execute(f"CREATE SCHEMA {schema}")
            else: 
                print(f"Esquema {schema} já existe no banco {database}.")
        
        # Verificar se a tabela já existe no esquema maloka_analytics
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='analise_estoque' AND table_schema='maloka_analytics')")
        tabela_analytics_existe = cursor.fetchone()[0]
        
        # Verificar se a tabela já existe no esquema maloka_core
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='analise_estoque' AND table_schema='maloka_core')")
        tabela_core_existe = cursor.fetchone()[0]
        
        # Função auxiliar para verificar e atualizar uma tabela em um determinado esquema
        def verificar_atualizar_tabela(schema, tabela_existe):
            if tabela_existe:
                # Verificar se as novas colunas existem na tabela
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='analise_estoque' AND table_schema='{schema}'")
                colunas_existentes = [row[0] for row in cursor.fetchall()]
                
                # Adicionar colunas que não existem ainda
                for coluna in estoque_com_vendas.columns:
                    if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                        print(f"Adicionando nova coluna: {coluna} em {schema}.analise_estoque")
                        
                        # Determinar o tipo de dados da coluna
                        dtype = estoque_com_vendas[coluna].dtype
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
                            cursor.execute(f'ALTER TABLE {schema}.analise_estoque ADD COLUMN "{coluna}" {tipo}')
                            conn.commit()
                            print(f"Coluna {coluna} adicionada com sucesso em {schema}!")
                        except Exception as e:
                            print(f"Erro ao adicionar coluna {coluna} em {schema}: {e}")
                            conn.rollback()
                    
                # Limpar os dados existentes
                print(f"Limpando dados existentes na tabela {schema}.analise_estoque...")
                cursor.execute(f"TRUNCATE TABLE {schema}.analise_estoque")
                conn.commit()
            else:
                # Criar a tabela se não existir
                print(f"Criando tabela analise_estoque no esquema {schema}...")
                # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
                colunas = []
                for coluna, dtype in estoque_com_vendas.dtypes.items():
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
                    colunas.append(f'"{coluna}" {tipo}')
                
                create_table_query = f"""
                CREATE TABLE {schema}.analise_estoque (
                    {", ".join(colunas)}
                )
                """
                cursor.execute(create_table_query)
                conn.commit()
                
        # Verificar e atualizar tabela em maloka_analytics
        verificar_atualizar_tabela('maloka_analytics', tabela_analytics_existe)
        
        # Verificar e atualizar tabela em maloka_core
        verificar_atualizar_tabela('maloka_core', tabela_core_existe)
        
        # Função auxiliar para inserir dados em um esquema específico
        def inserir_dados_no_esquema(schema):
            # Otimização da inserção de dados para a tabela de análise de estoque
            print(f"Inserindo {len(estoque_com_vendas)} registros na tabela {schema}.analise_estoque...")
            
            # Preparar as colunas para inserção
            colunas = [f'"{col}"' for col in estoque_com_vendas.columns]
            
            # Otimizar a tabela temporariamente para inserção rápida
            try:
                print(f"Otimizando configurações da tabela {schema}.analise_estoque para inserção rápida...")
                cursor.execute(f"ALTER TABLE {schema}.analise_estoque SET UNLOGGED")
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET synchronous_commit = off")
            except Exception as e:
                print(f"Aviso ao otimizar tabela {schema}.analise_estoque: {e}")
            
            # Método 1: Usar COPY FROM (Pull Processing)
            try:
                print(f"Tentando inserção usando COPY FROM para {schema}.analise_estoque (método mais rápido)...")
                
                # Criar arquivo temporário para o COPY
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    temp_path = temp_file.name
                    # Salvar DataFrame para CSV temporário
                    estoque_com_vendas.to_csv(temp_path, index=False, header=False, na_rep='\\N')
                
                # Abrir arquivo em modo de leitura
                with open(temp_path, 'r') as f:
                    # Usar COPY para inserir dados
                    cursor.copy_expert(
                        f"COPY {schema}.analise_estoque ({', '.join(colunas)}) FROM STDIN WITH CSV",
                        f
                    )
                
                # Commit uma única vez
                conn.commit()
                
                # Remover arquivo temporário apenas na última iteração
                if schema == 'maloka_core':
                    os.unlink(temp_path)
                
                print(f"Todos os {len(estoque_com_vendas)} registros inseridos com sucesso via COPY FROM em {schema}!")
                return True
                
            except Exception as e:
                print(f"Erro ao usar COPY FROM para {schema}.analise_estoque: {e}")
                print(f"Tentando método alternativo usando multiprocessing para {schema}...")
                
                # Reverter qualquer transação pendente
                conn.rollback()
                
                # Método 2: Usar multiprocessing para INSERT paralelo
                
                # Converter NaN para None
                df_upload = estoque_com_vendas.replace({np.nan: None})
                
                # Preparar a query para INSERT
                placeholders = ", ".join(["%s"] * len(df_upload.columns))
                insert_query = f"""
                INSERT INTO {schema}.analise_estoque ({", ".join(colunas)})
                VALUES ({placeholders})
                """
                return False, insert_query, df_upload
        
        # Inserir dados em maloka_analytics
        success_analytics = inserir_dados_no_esquema('maloka_analytics')
        
        # Se o método COPY FROM falhou, precisamos usar o método multiprocessing
        if not isinstance(success_analytics, bool):
            _, insert_query_analytics, df_upload = success_analytics
        else:
            insert_query_analytics = None
            df_upload = None
        
        # Inserir dados em maloka_core
        success_core = inserir_dados_no_esquema('maloka_core')
        
        # Se o método COPY FROM falhou para maloka_core, usar o método multiprocessing
        if not isinstance(success_core, bool):
            _, insert_query_core, df_upload = success_core
        
        # Se precisamos usar o método multiprocessing em algum dos esquemas
        if insert_query_analytics or (not isinstance(success_core, bool) and not isinstance(success_analytics, bool)):
            # Preparamos os dados para multiprocessing apenas uma vez
            
            # Função auxiliar para fazer inserção com multiprocessing
            def inserir_com_multiprocessing(schema, insert_query):
                print(f"Iniciando inserção com multiprocessing para {schema}.analise_estoque...")
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
                print(f"Iniciando inserção paralela com {len(lotes)} lotes para {schema}...")
                
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
                            
                return total_inseridos, erros, start_time
            
            # Se precisamos usar multiprocessing para qualquer um dos esquemas
            if insert_query_analytics is not None or not isinstance(success_core, bool):
                # Variáveis para rastrear resultados gerais
                total_registros_processados = 0
                total_registros_inseridos = 0
                todos_erros = []
                
                # Se precisamos usar multiprocessing para maloka_analytics
                if insert_query_analytics is not None:
                    total_inseridos_analytics, erros_analytics, start_time_analytics = inserir_com_multiprocessing(
                        'maloka_analytics', insert_query_analytics)
                    
                    elapsed_analytics = (datetime.now() - start_time_analytics).total_seconds()
                    print(f"Inseridos {total_inseridos_analytics} registros em maloka_analytics em {elapsed_analytics:.2f} segundos")
                    
                    if erros_analytics:
                        print(f"Ocorreram {len(erros_analytics)} erros durante a inserção em maloka_analytics")
                        for erro in erros_analytics[:5]:  # Mostrar apenas os 5 primeiros erros
                            print(f"- {erro}")
                    
                    total_registros_inseridos += total_inseridos_analytics
                    todos_erros.extend(erros_analytics)
                
                # Se precisamos usar multiprocessing para maloka_core
                if not isinstance(success_core, bool):
                    total_inseridos_core, erros_core, start_time_core = inserir_com_multiprocessing(
                        'maloka_core', insert_query_core)
                    
                    elapsed_core = (datetime.now() - start_time_core).total_seconds()
                    print(f"Inseridos {total_inseridos_core} registros em maloka_core em {elapsed_core:.2f} segundos")
                    
                    if erros_core:
                        print(f"Ocorreram {len(erros_core)} erros durante a inserção em maloka_core")
                        for erro in erros_core[:5]:  # Mostrar apenas os 5 primeiros erros
                            print(f"- {erro}")
                    
                    total_registros_inseridos += total_inseridos_core
                    todos_erros.extend(erros_core)
                
                # Calcular o total de registros processados
                total_registros_processados = len(df_upload) * (
                    (1 if insert_query_analytics is not None else 0) + 
                    (1 if not isinstance(success_core, bool) else 0)
                )
                
                if total_registros_inseridos < total_registros_processados:
                    print(f"Atenção: {total_registros_processados - total_registros_inseridos} registros não foram inseridos")
                else:
                    print("Todos os registros foram inseridos com sucesso em ambos os esquemas!")
            else:
                print("Todos os registros foram inseridos com sucesso via COPY FROM em ambos os esquemas!")
        
        # Restaurar configurações da tabela e otimizar para consultas
        print("Restaurando configurações e otimizando para consultas...")
        try:
            # Restaurar configurações do PostgreSQL
            cursor.execute("SET maintenance_work_mem = '64MB'")  # Valor padrão
            cursor.execute("SET synchronous_commit = on")  # Valor padrão
            
            # Restaurar configurações para ambos os esquemas
            for schema in ['maloka_analytics', 'maloka_core']:
                try:
                    # Converter de volta para LOGGED para garantir durabilidade
                    cursor.execute(f"ALTER TABLE {schema}.analise_estoque SET LOGGED")
                    print(f"Tabela {schema}.analise_estoque convertida para LOGGED com sucesso")
                    
                    # # Criar índices para melhorar performance de consultas
                    # print(f"Criando índices para otimizar consultas futuras em {schema}...")
                    # cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_analise_estoque_id_sku ON {schema}.analise_estoque (id_sku)")
                    # cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_analise_estoque_curva_abc ON {schema}.analise_estoque (curva_abc)")
                    # cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_analise_estoque_situacao ON {schema}.analise_estoque (situacao_do_produto)")
                    
                    # Analisar tabela para otimizar planejamento de consultas
                    # print(f"Analisando tabela {schema}.analise_estoque para otimizar consultas...")
                    # cursor.execute(f"ANALYZE {schema}.analise_estoque")
                    
                except Exception as e:
                    print(f"Aviso ao restaurar configurações para {schema}: {e}")
            
        except Exception as e:
            print(f"Aviso ao restaurar configurações gerais: {e}")
        
        print(f"Dados de análise de estoque inseridos com sucesso! Total de {len(estoque_com_vendas)} registros.")
        
        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao inserir dados no banco: {e}")
        if 'conn' in locals() and conn is not None:
            conn.close()
            
    print(f"Análise de estoque concluída para {nome_cliente}")

    # # Exportar para CSV - acrescentar ao arquivo existente
    # caminho_arquivo_metricas = os.path.join(diretorio_cliente, f'{nome_cliente}_metricas_analise_estoque.csv')
    # # Verificar se o arquivo já existe
    # if os.path.exists(caminho_arquivo_metricas):
    #     # Carregar o arquivo existente
    #     df_metricas_existente = pd.read_csv(caminho_arquivo_metricas)
        
    #     # Concatenar o DataFrame existente com as novas métricas
    #     df_metricas_atualizado = pd.concat([df_metricas_existente, df_metricas], ignore_index=True)
        
    #     ###############################
    #     # Salvar relatório
    #     ###############################
    #     # Salvar o DataFrame atualizado
    #     # df_metricas_atualizado.to_csv(caminho_arquivo_metricas, index=False)
    #     print(f"\nMétricas de hoje adicionadas ao cliente {nome_cliente}")
    #     print(f"Total de registros no arquivo: {len(df_metricas_atualizado)}")
    # else:
    #     # Se o arquivo não existir, criar novo
    #     df_metricas.to_csv(caminho_arquivo_metricas, index=False)
    #     print(f"\nArquivo de métricas criado para {nome_cliente}")
    
    print("\n=== EXPORTANDO MÉTRICAS DE ESTOQUE PARA O BANCO DE DADOS ===")
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
        
        # Criar cursor
        cursor = conn.cursor()
        
        # Verificar se o esquema maloka_analytics existe, caso contrário, criar
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'maloka_analytics')")
        schema_existe = cursor.fetchone()[0]
        
        if not schema_existe:
            print(f"Esquema maloka_analytics não existe no banco {database}. Criando...")
            cursor.execute("CREATE SCHEMA maloka_analytics")
        
        # Verificar se a tabela metricas_estoque já existe no esquema maloka_analytics
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='metricas_estoque' AND table_schema='maloka_analytics')")
        tabela_existe = cursor.fetchone()[0]
        
        if not tabela_existe:
            # Criar a tabela se não existir
            print("Criando tabela analise_estoque no esquema maloka_analytics...")
            # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
            # Otimizando os tipos de dados para melhor performance
            colunas = []
            for coluna, dtype in estoque_com_vendas.dtypes.items():
                if 'int64' in str(dtype):
                    tipo = 'BIGINT'  # Usar BIGINT para inteiros grandes
                elif 'int' in str(dtype):
                    tipo = 'INTEGER'
                elif 'float' in str(dtype):
                    tipo = 'NUMERIC'  # NUMERIC é melhor para precisão
                elif 'datetime' in str(dtype):
                    tipo = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    tipo = 'BOOLEAN'
                else:
                    tipo = 'TEXT'
                colunas.append(f'"{coluna}" {tipo}')
            
            # Adicionando uma coluna id como chave primária
            create_table_query = f"""
            CREATE TABLE maloka_analytics.metricas_estoque (
                id SERIAL PRIMARY KEY,
                cliente VARCHAR(100),
                {", ".join(colunas)}
            )
            """
            cursor.execute(create_table_query)
            print("Tabela metricas_estoque criada com sucesso!")
        
        # Extrair apenas a data (sem a hora) da data_hora_analise
        data_atual = metricas['data_hora_analise'].split()[0]
        
        # Verificar se já existem métricas para o mesmo dia
        cursor.execute(f"""
            SELECT id FROM maloka_analytics.metricas_estoque 
            WHERE cliente = '{nome_cliente}' AND CAST(data_hora_analise AS DATE) = '{data_atual}'
        """)
        
        ids_existentes = cursor.fetchall()
        
        if ids_existentes:
            # Excluir registros existentes para o mesmo dia
            ids_para_excluir = [str(id[0]) for id in ids_existentes]
            print(f"Encontrado(s) {len(ids_para_excluir)} registro(s) de métricas para {nome_cliente} na data {data_atual}. Substituindo...")
            
            cursor.execute(f"""
                DELETE FROM maloka_analytics.metricas_estoque 
                WHERE id IN ({','.join(ids_para_excluir)})
            """)
            conn.commit()
        
        print(f"Inserindo novas métricas para o cliente {nome_cliente}...")
        
        # Converter NaN para None
        df_upload = df_metricas.replace({np.nan: None})
        
        # Preparar a query para inserção
        colunas = [f'"{col}"' for col in df_upload.columns]
        placeholders = ", ".join(["%s"] * len(df_upload.columns))
        insert_query = f"""
        INSERT INTO maloka_analytics.metricas_estoque (cliente, {", ".join(colunas)})
        VALUES (%s, {placeholders})
        """
        
        # Criar uma lista de tuplas com os valores
        valores = [(nome_cliente,) + tuple(row) for _, row in df_upload.iterrows()]
        
        # Executar a inserção
        cursor.executemany(insert_query, valores)
        conn.commit()
        
        print(f"Métricas de estoque inseridas com sucesso para {nome_cliente}!")
        
        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao inserir métricas no banco: {e}")
        if 'conn' in locals() and conn is not None:
            conn.close()

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
            gerar_relatorios_estoque(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_relatorios_estoque(args.cliente)


"""
Para executar um cliente específico, use o comando:
python analise_estoque.py nome_do_cliente

Para executar para todos os clientes sem especificar argumentos, use:
python analise_estoque.py
"""