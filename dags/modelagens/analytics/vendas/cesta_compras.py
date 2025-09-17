from collections import Counter
from datetime import datetime
import pandas as pd
import os
import warnings
import psycopg2
import numpy as np
import argparse
import sys
from prefixspan import PrefixSpan
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import DB_CONFIG_MALOKA

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def recomendar_cesta_personalizada(df_vendas_completo, df_produto, df_cliente):
    """
    Gera recomendações de cesta de compras personalizadas para cada cliente
    com base em seu histórico de compras usando PrefixSpan.
    """
    print("\nGerando recomendações de cestas personalizadas para clientes...")
    
    # Mapear produtos para IDs sequenciais para uso no PrefixSpan
    produtos_unicos = df_vendas_completo['id_produto'].unique()
    mapa_produto = {produto_id: idx for idx, produto_id in enumerate(produtos_unicos)}
    mapa_id_para_produto = {idx: produto_id for produto_id, idx in mapa_produto.items()}
    
    # Criar mapeamento de ID para nome do produto
    mapa_id_para_nome = dict(zip(df_produto['id_produto'], df_produto['nome_produto']))
    
    # Agrupar por cliente para analisar histórico individual
    clientes_unicos = df_vendas_completo['id_cliente'].unique()
    
    # Preparar DataFrame para armazenar recomendações
    recomendacoes_clientes = []

    # Contadores para estatísticas
    total_clientes = len(clientes_unicos)
    clientes_poucas_transacoes = 0
    clientes_poucas_sequencias = 0
    clientes_erros = 0
    
    print(f"Processando {total_clientes} clientes...")
    
    # Para cada cliente
    for i, cliente_id in enumerate(clientes_unicos, 1):
        if i % 100 == 0:
            print(f"Processando cliente {i}/{len(clientes_unicos)}")
            
        try:
            # Filtrar vendas do cliente
            vendas_cliente = df_vendas_completo[df_vendas_completo['id_cliente'] == cliente_id].copy()
            
            # Obter nome do cliente do dataframe de clientes, caso contrário usar ID
            nome_cliente = None
            if 'nome_cliente' in vendas_cliente.columns:
                nome_cliente = vendas_cliente['nome_cliente'].iloc[0] 
            elif df_cliente is not None:
                # Buscar nome no df_cliente
                cliente_info = df_cliente[df_cliente['id_cliente'] == cliente_id]
                if not cliente_info.empty and 'nome_cliente' in cliente_info.columns:
                    nome_cliente = cliente_info['nome_cliente'].iloc[0]

            # Se ainda não encontrou o nome, usar ID como fallback
            if not nome_cliente:
                nome_cliente = f"Cliente_{cliente_id}"
            
            # Verificar número de transações únicas
            transacoes_unicas = vendas_cliente['id_venda'].unique()
            if len(transacoes_unicas) < 5:
                clientes_poucas_transacoes += 1
                continue
            
            # Ordenar por data para manter sequência temporal
            if 'data_venda' in vendas_cliente.columns:
                vendas_cliente = vendas_cliente.sort_values('data_venda')
                
            # Preparar sequências de transações para este cliente
            sequencias_cliente = []
            
            for venda_id in transacoes_unicas:
                produtos_da_venda = vendas_cliente[vendas_cliente['id_venda'] == venda_id]
                
                # Mapear produtos para IDs sequenciais
                produtos_comprados = []
                for produto_id in produtos_da_venda['id_produto'].tolist():
                    if produto_id in mapa_produto:
                        produtos_comprados.append(mapa_produto[produto_id])
                
                # Adicionar apenas transações com pelo menos 1 produto
                if produtos_comprados:
                    # Ordenar produtos dentro da transação para consistência
                    produtos_comprados.sort()
                    sequencias_cliente.append(produtos_comprados)
            
            # Pular cliente se não tiver sequências suficientes
            if len(sequencias_cliente) < 3:
                clientes_poucas_sequencias += 1
                continue
                
            # Aplicar PrefixSpan para este cliente específico
            try:
                ps = PrefixSpan(sequencias_cliente)
                suporte_minimo = max(2, len(sequencias_cliente) // 3)  # Ajustar suporte baseado no histórico
                padroes = ps.frequent(suporte_minimo)
                
                # Filtrar padrões relevantes (com pelo menos 2 produtos)
                padroes_relevantes = []
                for suporte, padrao in padroes:
                    if len(padrao) >= 2 and len(padrao) <= 5:  # Padrões entre 2 e 5 produtos
                        padroes_relevantes.append((suporte, padrao))
                
                # Se encontrou padrões relevantes
                if padroes_relevantes:
                    # Ordenar por suporte (frequência) e tamanho do padrão
                    padroes_relevantes.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
                    
                    # Pegar o melhor padrão
                    melhor_suporte, melhor_padrao = padroes_relevantes[0]
                    
                    # Extrair produtos do padrão
                    produtos_recomendados = []
                    for item in melhor_padrao:
                        if isinstance(item, (list, tuple)):
                            # Se o item é uma sequência, pegar todos os produtos
                            for sub_item in item:
                                if isinstance(sub_item, (int, np.integer)) and sub_item in mapa_id_para_produto:
                                    produto_real_id = mapa_id_para_produto[sub_item]
                                    nome_produto = mapa_id_para_nome.get(produto_real_id, f"Produto_{produto_real_id}")
                                    # Formatar como nome_produto_id_produto
                                    produto_formatado = f"{nome_produto}_{produto_real_id}"
                                    if produto_formatado not in produtos_recomendados:
                                        produtos_recomendados.append(produto_formatado)
                        elif isinstance(item, (int, np.integer)) and item in mapa_id_para_produto:
                            # Se o item é um produto direto
                            produto_real_id = mapa_id_para_produto[item]
                            nome_produto = mapa_id_para_nome.get(produto_real_id, f"Produto_{produto_real_id}")
                            # Formatar como nome_produto_id_produto
                            produto_formatado = f"{nome_produto}_{produto_real_id}"
                            if produto_formatado not in produtos_recomendados:
                                produtos_recomendados.append(produto_formatado)
                    
                    # Calcular confiança baseada no suporte
                    confianca = (melhor_suporte / len(sequencias_cliente)) * 100
                    
                    # Adicionar recomendação para este cliente
                    if produtos_recomendados:
                        recomendacoes_clientes.append({
                            'id_cliente': cliente_id,
                            'nome_cliente': nome_cliente,
                            'produtos_recomendados': produtos_recomendados[:5],  # Limitar a 5 produtos
                            'quantidade_recomendados': len(produtos_recomendados[:5]),
                            'confianca': f"{confianca:.1f}%",
                            'num_transacoes': len(sequencias_cliente),
                            'metodo': 'PrefixSpan'
                        })
                        
                else:
                    # Fallback: produtos mais comprados pelo cliente
                    produtos_mais_comprados = vendas_cliente['id_produto'].value_counts().head(3)
                    produtos_fallback = []
                    
                    for produto_id, freq in produtos_mais_comprados.items():
                        nome_produto = mapa_id_para_nome.get(produto_id, f"Produto_{produto_id}")
                        # Formatar como nome_produto_id_produto
                        produto_formatado = f"{nome_produto}_{produto_id}"
                        produtos_fallback.append(produto_formatado)
                    
                    if produtos_fallback:
                        recomendacoes_clientes.append({
                            'id_cliente': cliente_id,
                            'nome_cliente': nome_cliente,
                            'produtos_recomendados': produtos_fallback,
                            'quantidade_recomendados': len(produtos_fallback),
                            'confianca': "N/A",
                            'num_transacoes': len(sequencias_cliente),
                            'metodo': 'Mais comprados'
                        })
                        
            except Exception as e:
                print(f"Erro PrefixSpan para cliente {cliente_id}: {e}")
                clientes_erros += 1
                continue
        
        except Exception as e:
            print(f"Erro ao processar cliente {cliente_id}: {e}")
            clientes_erros += 1
            continue
    
    # Criar DataFrame com recomendações por cliente
    if recomendacoes_clientes:
        df_recomendacoes_clientes = pd.DataFrame(recomendacoes_clientes)
        
        # Estatísticas
        total_recomendacoes = len(df_recomendacoes_clientes)
        prefixspan_count = len(df_recomendacoes_clientes[df_recomendacoes_clientes['metodo'] == 'PrefixSpan'])
        fallback_count = total_recomendacoes - prefixspan_count
        
        print(f"\nRecomendações geradas para {total_recomendacoes} clientes:")
        print(f"- PrefixSpan: {prefixspan_count} clientes")
        print(f"- Fallback (mais comprados): {fallback_count} clientes")
        print(f"\nClientes filtrados:")
        print(f"- Com menos de 10 transações: {clientes_poucas_transacoes} clientes")
        print(f"- Com menos de 3 sequências válidas: {clientes_poucas_sequencias} clientes")
        print(f"- Com erros durante o processamento: {clientes_erros} clientes")
        print(f"- Total de clientes iniciais: {total_clientes}")
        
        # Mostrar alguns exemplos
        # print("\nExemplos de recomendações:")
        # print("-" * 60)
        # for idx, row in df_recomendacoes_clientes.head(3).iterrows():
        #     produtos_str = ", ".join(row['produtos_recomendados'])
        #     print(f"Cliente: {row['nome_cliente']}")
        #     print(f"Produtos: {produtos_str}")
        #     print(f"Confiança: {row['confianca']} | Método: {row['metodo']}")
        #     print("-" * 30)
        
        return df_recomendacoes_clientes
    else:
        print("Não foi possível gerar recomendações personalizadas para os clientes.")
        print(f"\nClientes filtrados:")
        print(f"- Com menos de 10 transações: {clientes_poucas_transacoes} clientes")
        print(f"- Com menos de 3 sequências válidas: {clientes_poucas_sequencias} clientes")
        print(f"- Com erros durante o processamento: {clientes_erros} clientes")
        print(f"- Total de clientes iniciais: {total_clientes}")
        return None

def gerar_relatorios_cesta_compras(nome_cliente):
    """
    Gera relatórios de cesta de compras.
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

    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    
    # Criar diretório para salvar os relatórios do cliente
    diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_cesta_compras', nome_cliente)
    os.makedirs(diretorio_cliente, exist_ok=True)
    
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
        SELECT id_venda, id_cliente, data_venda, total_venda, id_loja
        FROM {schema}.venda
        """
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_vendas = pd.read_sql_query(query, conn)
        
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

    # Preparar dados para modelo prefixspan
    print("\nPreparando dados para análise sequencial com PrefixSpan...")

    try:
        # Etapa 1: Juntar as tabelas para obter dados completos de vendas
        print("Mesclando tabelas de vendas, itens e produtos...")
        
        # Converter data_venda para datetime se não estiver
        if not pd.api.types.is_datetime64_any_dtype(df_vendas['data_venda']):
            df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
        
        # Pegar somente os itens com tipo venda igual a P
        df_venda_itens = df_venda_itens[df_venda_itens['tipo'] == 'P']
            
        # Juntar vendas com itens de venda
        df_vendas_completo = pd.merge(
            df_vendas,
            df_venda_itens,
            on='id_venda',
            how='inner'
        )
        
        # Juntar com informações de produto
        df_vendas_completo = pd.merge(
            df_vendas_completo,
            df_produto,
            on='id_produto',
            how='inner'
        )
        
        # Etapa 2: Ordenar vendas por cliente e data
        print("Ordenando transações por cliente e data/hora...")
        df_vendas_completo = df_vendas_completo.sort_values(by=['id_cliente', 'data_venda'])
        
        # Etapa 3: Mapear IDs para valores sequenciais (opcional, para simplificar a representação)
        print("Mapeando IDs de clientes e produtos para valores sequenciais...")
        # Mapear clientes para IDs sequenciais
        clientes_unicos = df_vendas_completo['id_cliente'].unique()
        mapa_cliente = {cliente_id: idx for idx, cliente_id in enumerate(clientes_unicos)}
        df_vendas_completo['cliente_seq_id'] = df_vendas_completo['id_cliente'].map(mapa_cliente)
        
        # Mapear produtos para IDs sequenciais
        produtos_unicos = df_vendas_completo['id_produto'].unique()
        mapa_produto = {produto_id: idx for idx, produto_id in enumerate(produtos_unicos)}
        df_vendas_completo['produto_seq_id'] = df_vendas_completo['id_produto'].map(mapa_produto)
        
        # Etapa 4: Agrupar por cliente para criar sequências
        print("Agrupando compras por cliente...")
        sequencias_cliente = {}
        
        # Filtrar vendas do último ano
        data_atual = datetime.now()
        um_ano_atras = data_atual - pd.DateOffset(years=1)
        
        print(f"Filtrando vendas a partir de {um_ano_atras.strftime('%d/%m/%Y')}")
        df_vendas_ultimo_ano = df_vendas_completo[df_vendas_completo['data_venda'] >= um_ano_atras]
        
        # Selecionar apenas clientes que compraram no último ano
        clientes_ultimo_ano = df_vendas_ultimo_ano['id_cliente'].unique()
        print(f"Encontrados {len(clientes_ultimo_ano)} clientes com compras no último ano.")
        
        # Contador para estatísticas
        total_transacoes = 0
        transacoes_multiprodutos = 0
        transacoes_removidas = 0
        
        for cliente_id in clientes_ultimo_ano:
            # Filtrar vendas do cliente (ainda considerando todo o histórico para análise)
            vendas_cliente = df_vendas_completo[df_vendas_completo['id_cliente'] == cliente_id]
            
            # Agrupar por transação (id_venda)
            transacoes = []
            for venda_id, grupo in vendas_cliente.groupby('id_venda'):
                # Lista de produtos comprados nesta transação
                produtos_na_transacao = grupo['produto_seq_id'].tolist()
                
                total_transacoes += 1
                
                # Filtrar apenas transações com mais de um produto
                if len(produtos_na_transacao) > 1:
                    transacoes.append(produtos_na_transacao)
                    transacoes_multiprodutos += 1
                else:
                    transacoes_removidas += 1
            
            # Armazenar a sequência de transações deste cliente (se houver alguma válida)
            if transacoes:  # Adicionar apenas se houver transações válidas
                sequencias_cliente[cliente_id] = transacoes
        
        print(f"Dados preparados: {len(sequencias_cliente)} sequências de clientes criadas.")
        print(f"Total de transações: {total_transacoes}")
        print(f"Transações com múltiplos produtos: {transacoes_multiprodutos} ({transacoes_multiprodutos/total_transacoes*100:.1f}%)")
        print(f"Transações com apenas um produto (removidas): {transacoes_removidas} ({transacoes_removidas/total_transacoes*100:.1f}%)")

        # Criar mapeamento de ID para nome do produto
        mapa_id_para_nome = dict(zip(df_produto['id_produto'], df_produto['nome_produto']))

        try:
            # Gerar recomendações personalizadas por cliente
            df_recomendacoes_clientes = recomendar_cesta_personalizada(df_vendas_ultimo_ano, df_produto, df_cliente)
            
            if df_recomendacoes_clientes is not None:
                # Salvar recomendações personalizadas
                caminho_recomendacoes_cliente = os.path.join(diretorio_cliente, 'recomendacoes_por_cliente.csv')
                
                # Converter a coluna de produtos recomendados para string para facilitar a exportação
                df_export = df_recomendacoes_clientes.copy()
                df_export['produtos_recomendados'] = df_export['produtos_recomendados'].apply(lambda x: ", ".join(x))
                
                df_export.to_csv(caminho_recomendacoes_cliente, index=False)
                print(f"Recomendações de cestas personalizadas salvas em: {caminho_recomendacoes_cliente}")
                
                # Gerar arquivo de validação com as 3 últimas compras dos clientes recomendados
                print("\nGerando dados de validação das recomendações...")
                
                # Lista para armazenar dados das últimas compras
                dados_validacao = []
                
                # Obter a lista de clientes que receberam recomendações
                clientes_com_recomendacao = df_recomendacoes_clientes['id_cliente'].unique()
                
                print(f"Extraindo histórico recente de {len(clientes_com_recomendacao)} clientes para validação...")
                
                # Para cada cliente que recebeu recomendação
                for cliente_id in clientes_com_recomendacao:
                    # Filtrar vendas do cliente
                    vendas_cliente = df_vendas_ultimo_ano[df_vendas_ultimo_ano['id_cliente'] == cliente_id].copy()
                    
                    # Obter nome do cliente
                    nome_cliente = df_recomendacoes_clientes[df_recomendacoes_clientes['id_cliente'] == cliente_id]['nome_cliente'].iloc[0]
                    
                    # Ordenar por data decrescente para pegar as mais recentes
                    if 'data_venda' in vendas_cliente.columns:
                        vendas_cliente = vendas_cliente.sort_values('data_venda', ascending=False)
                    
                    # Pegar as 3 transações mais recentes
                    ultimas_vendas_ids = vendas_cliente['id_venda'].unique()[:3]
                    
                    # Para cada uma das últimas vendas
                    for i, venda_id in enumerate(ultimas_vendas_ids, 1):
                        itens_venda = vendas_cliente[vendas_cliente['id_venda'] == venda_id]
                        
                        if 'data_venda' in itens_venda.columns:
                            data_venda = itens_venda['data_venda'].iloc[0]
                        else:
                            data_venda = None
                        
                        # Obter produtos comprados nesta transação
                        produtos = []
                        for _, item in itens_venda.iterrows():
                            produto_id = item['id_produto']
                            nome_produto = mapa_id_para_nome.get(produto_id, f"Produto_{produto_id}")
                            # Formatar como nome_produto_id_produto
                            produto_formatado = f"{nome_produto}_{produto_id}"
                            produtos.append(produto_formatado)
                        
                        # Adicionar aos dados de validação
                        dados_validacao.append({
                            'id_cliente': cliente_id,
                            'nome_cliente': nome_cliente,
                            'id_venda': venda_id,
                            'data_venda': data_venda,
                            'ordem_recente': i,  # 1 = mais recente, 2 = segunda mais recente, etc.
                            'produtos_comprados': ", ".join(produtos)
                        })
                
                # Criar DataFrame de validação
                if dados_validacao:
                    df_validacao = pd.DataFrame(dados_validacao)
                    
                    # Ordenar por cliente e ordem de recência
                    df_validacao = df_validacao.sort_values(['id_cliente', 'ordem_recente'])
                    
                    # Salvar arquivo de validação
                    caminho_validacao = os.path.join(diretorio_cliente, 'validacao_ultimas_compras.csv')
                    df_validacao.to_csv(caminho_validacao, index=False)
                    
                    print(f"Dados de validação com últimas compras salvos em: {caminho_validacao}")
                    print(f"Total de {len(df_validacao)} transações recentes para validação.")
                else:
                    print("Não foi possível gerar dados de validação.")

        except Exception as e:
            print(f"Erro ao gerar recomendações para cada cliente: {e}")
            
    except Exception as e:
        print(f"Erro ao preparar dados para PrefixSpan: {e}")
        return None, None

if __name__ == "__main__":
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Gera relatórios de cesta de compras para um cliente específico')
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
            gerar_relatorios_cesta_compras(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_relatorios_cesta_compras(args.cliente)

"""
Para executar um cliente específico, use o comando:
python cesta_compras.py nome_do_cliente

Para executar para todos os clientes, use o comando:
python cesta_compras.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python cesta_compras.py
"""