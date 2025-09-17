from datetime import datetime
import pandas as pd
import os
import warnings
import psycopg2
import argparse
import sys
import numpy as np
import traceback
from multiprocessing import Pool, cpu_count
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

def inserir_dados_paralelo(df, tabela, database, nome_cliente, conn, cursor):
    """
    Função para inserir dados em uma tabela usando primeiro o método COPY (mais rápido) 
    e depois multiprocessing como fallback.
    
    Args:
        df: DataFrame com os dados a serem inseridos
        tabela: Nome da tabela no banco de dados (sem o esquema)
        database: Nome do banco de dados
        nome_cliente: Nome do cliente (para mensagens de log)
        conn: Conexão com o banco de dados
        cursor: Cursor para execução de comandos SQL
        
    Returns:
        bool: True se a inserção foi bem-sucedida, False caso contrário
    """
    try:
        # Preparar as colunas para inserção
        colunas = [f'"{col}"' for col in df.columns]

        # Reverter qualquer transação pendente
        conn.rollback()
            
        # Usar multiprocessing para INSERT paralelo
        # Converter NaN para None
        df_upload = df.replace({np.nan: None})
            
        # Preparar a query
        placeholders = ", ".join(["%s"] * len(df_upload.columns))
        insert_query = f"""
        INSERT INTO maloka_analytics.{tabela} ({", ".join(colunas)})
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
        print(f"Iniciando inserção paralela para {tabela} com {len(lotes)} lotes...")
            
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
                print(f"Inseridos {total_inseridos} de {len(valores)} registros de {tabela} em {elapsed:.2f} segundos")
                
            if erros:
                print(f"Ocorreram {len(erros)} erros durante a inserção em {tabela}")
                for erro in erros[:5]:  # Mostrar apenas os 5 primeiros erros
                    print(f"- {erro}")
            
        if total_inseridos < len(valores):
            print(f"Atenção: {len(valores) - total_inseridos} registros de {tabela} não foram inseridos")
                
            # Restaurar configurações
            try:
                cursor.execute("SET maintenance_work_mem = '64MB'")  # Valor padrão
                cursor.execute("SET synchronous_commit = on")  # Valor padrão
                cursor.execute(f"ALTER TABLE maloka_analytics.{tabela} SET LOGGED")
            except Exception as e:
                print(f"Aviso ao restaurar configurações: {e}")
                
            return False
        else:
            print(f"Todos os registros de {tabela} foram inseridos com sucesso!")
                
            # Restaurar configurações e otimizar para consultas
            print("Restaurando configurações e otimizando para consultas...")
            try:
                # Restaurar configurações do PostgreSQL
                cursor.execute("SET maintenance_work_mem = '64MB'")  # Valor padrão
                cursor.execute("SET synchronous_commit = on")  # Valor padrão
                    
                # Converter de volta para LOGGED para garantir durabilidade
                cursor.execute(f"ALTER TABLE maloka_analytics.{tabela} SET LOGGED")
                    
                # Analisar tabela para otimizar planejamento de consultas
                print(f"Analisando tabela {tabela} para otimizar consultas...")
                cursor.execute(f"ANALYZE maloka_analytics.{tabela}")
                    
            except Exception as e:
                    print(f"Aviso ao restaurar configurações: {e}")
                
            return True
    
    except Exception as e:
        print(f"Erro durante a inserção em {tabela}: {e}")
        print("Tentando método tradicional...")
        
        try:
            # Método tradicional como último recurso
            df_upload = df.replace({np.nan: None})
            colunas = [f'"{col}"' for col in df_upload.columns]
            placeholders = ", ".join(["%s"] * len(df_upload.columns))
            insert_query = f"""
            INSERT INTO maloka_analytics.{tabela} ({", ".join(colunas)})
            VALUES ({placeholders})
            """
            
            valores = [tuple(row) for _, row in df_upload.iterrows()]
            cursor.executemany(insert_query, valores)
            conn.commit()
            
            # Restaurar configurações
            try:
                cursor.execute("SET maintenance_work_mem = '64MB'")  # Valor padrão
                cursor.execute("SET synchronous_commit = on")  # Valor padrão
                cursor.execute(f"ALTER TABLE maloka_analytics.{tabela} SET LOGGED")
                cursor.execute(f"ANALYZE maloka_analytics.{tabela}")
            except Exception as e:
                print(f"Aviso ao restaurar configurações: {e}")
            
            print(f"Dados de {tabela} inseridos com sucesso usando método tradicional! Total de {len(df_upload)} registros.")
            return True
        except Exception as e2:
            print(f"Erro também no método tradicional: {e2}")
            return False

def gerar_relatorios_faturamento(nome_cliente):
    """
    Gera relatórios de faturamento para o cliente especificado
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
    # diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_faturamento', nome_cliente)
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
        
        print("Consultando a tabela vendas...")
        query = f"SELECT * FROM {schema}.venda"
        
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
        print("\nPrimeiros 5 registros para verificação:")
        print(df_vendas.head())
        
        ########################################################
        # consulta da tabela clientes
        ########################################################

        print("Consultando a tabela cliente...")
        query = f"SELECT * FROM {schema}.cliente"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_clientes = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_clientes)
        num_colunas = len(df_clientes.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_clientes.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_clientes.head())

        ########################################################
        # consulta da tabela venda_itens
        ########################################################
        
        print("Consultando a tabela venda_item...")
        query = f"SELECT * FROM {schema}.venda_item"
        
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
        print("\nPrimeiros 5 registros para verificação:")
        print(df_venda_itens.head())
        
        ########################################################
        # consulta da tabela loja
        ########################################################
        
        print("Consultando a tabela loja...")
        query = f"SELECT * FROM {schema}.loja"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_lojas = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_lojas)
        num_colunas = len(df_lojas.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        print(f"Colunas disponíveis: {', '.join(df_lojas.columns)}")
        
        # Exibir uma amostra dos dados
        print("\nPrimeiros 5 registros para verificação:")
        print(df_lojas.head())
        
        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")

        # Executa todas as análises com os dados carregados, mas salva os arquivos no diretório específico do cliente

        ########################################################
        # Faturamento Anual Consolidado (Produtos/Serviços + Cadastrado/Sem Cadastro)
        ########################################################

        #filtrar df_vendas para somente vendas que possuem a tipo_venda como PEDIDO e situacao_venda como CONCLUIDA
        df_vendas = df_vendas[(df_vendas['tipo_venda'] == 'PEDIDO') & (df_vendas['situacao_venda'] == 'CONCLUIDA')]

        # Incluindo o mesmo código de análise que você já tinha, mas ajustando os caminhos de salvamento
        # Primeiro, vamos preparar os dados de venda_itens
        df_venda_itens['tipo'] = df_venda_itens['tipo'].fillna('N/A')  # Tratando possíveis valores nulos

        # Mesclar df_vendas com df_venda_itens para obter as informações de tipo
        df_venda_itens_com_data = df_venda_itens.merge(
            df_vendas[['id_venda', 'data_venda']], 
            on='id_venda', 
            how='left'
        )

        # Converter data_venda para datetime e criar coluna Ano
        df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
        df_vendas['ano'] = df_vendas['data_venda'].dt.year
        df_venda_itens_com_data['data_venda'] = pd.to_datetime(df_venda_itens_com_data['data_venda'])
        df_venda_itens_com_data['ano'] = df_venda_itens_com_data['data_venda'].dt.year

        # Certifique-se de que total_item seja numérico
        df_venda_itens_com_data['total_item'] = pd.to_numeric(df_venda_itens_com_data['total_item'], errors='coerce')

        # Agrupar por Ano e tipo (Serviço ou Produto)
        df_anual_por_tipo_produto = df_venda_itens_com_data.groupby(['ano', 'tipo'])['total_item'].sum().unstack(fill_value=0)

        # Verificar se as colunas 'S' e 'P' existem, senão criar
        if 'S' not in df_anual_por_tipo_produto.columns:
            df_anual_por_tipo_produto['S'] = 0
        if 'P' not in df_anual_por_tipo_produto.columns:
            df_anual_por_tipo_produto['P'] = 0

        # Renomear as colunas para melhor clareza
        df_anual_por_tipo_produto.rename(columns={'S': 'faturameno_em_servicos', 'P': 'faturamento_em_produtos'}, inplace=True)

        # Calcular o total para cada ano
        df_anual_por_tipo_produto['total_de_faturamento'] = df_anual_por_tipo_produto.sum(axis=1)

        # Calcular as porcentagens
        for col in ['faturameno_em_servicos', 'faturamento_em_produtos']:
            if col in df_anual_por_tipo_produto.columns:
                df_anual_por_tipo_produto[f'percentual_{col}'] = (df_anual_por_tipo_produto[col] / df_anual_por_tipo_produto['total_de_faturamento']) * 100

        # Calcular a evolução percentual anual para cada categoria
        for col in ['faturameno_em_servicos', 'faturamento_em_produtos', 'total_de_faturamento']:
            if col in df_anual_por_tipo_produto.columns:
                df_anual_por_tipo_produto[f'percentual_evolução_{col}'] = df_anual_por_tipo_produto[col].pct_change() * 100

        # Adicionar contagem de vendas por tipo de produto (quantidade de itens)
        df_contagem_por_tipo = df_venda_itens_com_data.groupby(['ano', 'tipo']).size().unstack(fill_value=0)

        # Verificar se as colunas 'S' e 'P' existem, senão criar
        if 'S' not in df_contagem_por_tipo.columns:
            df_contagem_por_tipo['S'] = 0
        if 'P' not in df_contagem_por_tipo.columns:
            df_contagem_por_tipo['P'] = 0

        # Renomear as colunas para melhor clareza
        df_contagem_por_tipo.rename(columns={'S': 'qtd_vendas_servicos', 'P': 'qtd_vendas_produtos'}, inplace=True)

        # Resetar o índice para incluir 'Ano' como uma coluna
        df_anual_por_tipo_produto.reset_index(inplace=True)

        # Adicionar as contagens ao dataframe de faturamento
        df_anual_por_tipo_produto = df_anual_por_tipo_produto.merge(df_contagem_por_tipo, on='ano', how='left')

        # Calcular o total de itens por ano
        df_anual_por_tipo_produto['total_venda_itens'] = df_anual_por_tipo_produto[['qtd_vendas_servicos', 'qtd_vendas_produtos']].sum(axis=1)

        # Contar o número de vendas distintas por ano
        df_vendas_por_ano = df_vendas.groupby('ano').size().reset_index(name='qtd_vendas_ano')

        # Adicionar a contagem de vendas ao dataframe de análise anual
        df_anual_por_tipo_produto = df_anual_por_tipo_produto.merge(df_vendas_por_ano, on='ano', how='left')

        # Calcular o ticket médio anual (Total Faturamento / Qtd Vendas)
        df_anual_por_tipo_produto['ticket_medio_anual'] = df_anual_por_tipo_produto['total_de_faturamento'] / df_anual_por_tipo_produto['qtd_vendas_ano']

        # Calcular a evolução do ticket médio anual
        df_anual_por_tipo_produto['percentual_de_evolução_ticket_medio'] = df_anual_por_tipo_produto['ticket_medio_anual'].pct_change() * 100

        # Preparar a análise de cadastrados vs sem cadastro
        df_vendas_com_tipo_cliente = df_vendas.merge(df_clientes[['id_cliente', 'tipo']], on='id_cliente', how='left')

        # Classificamos baseado no tipo do cliente
        df_vendas_com_tipo_cliente['grupo_cliente'] = df_vendas_com_tipo_cliente['tipo'].apply(
            lambda tipo: 'faturamento_cliente_cadastrado' if tipo in ['F', 'J'] else 'faturamento_cliente_sem_cadastro'
        )

        # Agregar os dados por ano e grupo de cliente
        df_cadastro_ano = df_vendas_com_tipo_cliente.groupby(['ano', 'grupo_cliente'])['total_venda'].sum().unstack(fill_value=0)

        # Verificar se as colunas existem, senão criar
        if 'faturamento_cliente_cadastrado' not in df_cadastro_ano.columns:
            df_cadastro_ano['faturamento_cliente_cadastrado'] = 0
        if 'faturamento_cliente_sem_cadastro' not in df_cadastro_ano.columns:
            df_cadastro_ano['faturamento_cliente_sem_cadastro'] = 0

        # Resetar o índice para incluir 'ano' como uma coluna
        df_cadastro_ano.reset_index(inplace=True)

        # Calcular percentuais de clientes cadastrados e não cadastrados
        df_cadastro_ano['total_faturamento_clientes'] = df_cadastro_ano['faturamento_cliente_cadastrado'] + df_cadastro_ano['faturamento_cliente_sem_cadastro']
        df_cadastro_ano['percentual_faturamento_cliente_cadastrado'] = (df_cadastro_ano['faturamento_cliente_cadastrado'] / df_cadastro_ano['total_faturamento_clientes']) * 100
        df_cadastro_ano['percentual_faturamento_cliente_sem_cadastro'] = (df_cadastro_ano['faturamento_cliente_sem_cadastro'] / df_cadastro_ano['total_faturamento_clientes']) * 100
        
        # Agora vamos mesclar os dois DataFrames através da coluna 'ano'
        df_anual_consolidado = df_anual_por_tipo_produto.merge(df_cadastro_ano, on='ano', how='outer')

        # Verificar se há alguma coluna duplicada (como total_faturamento) e ajustar se necessário
        if 'total_de_faturamento' in df_anual_consolidado.columns and 'total_faturamento_clientes' in df_anual_consolidado.columns:
            # Apenas verificar se os valores são iguais (pode ser útil para validação)
            df_anual_consolidado['diferenca_totais'] = df_anual_consolidado['total_de_faturamento'] - df_anual_consolidado['total_faturamento_clientes']
            # Se quiser manter apenas um, pode usar:
            # df_anual_consolidado.drop('total_faturamento_clientes', axis=1, inplace=True)


        ###############################
        # Salvar relatório
        ###############################
        # Exportar o relatório consolidado
        # df_anual_consolidado.to_csv(os.path.join(diretorio_cliente, f'{nome_cliente}_faturamento_anual.csv'), index=False)
        print(f"Relatório de faturamento anual consolidado gerado para {nome_cliente}")

        ########################################################
        # Fatuarmento mensal por ano e loja
        ########################################################

        df_vendas['mes'] = df_vendas['data_venda'].dt.month

        anos_disponiveis = sorted(df_vendas['ano'].unique())
        lojas_disponiveis = sorted(df_vendas['id_loja'].unique())

        # Criar um DataFrame para consolidar todos os dados
        df_consolidado = pd.DataFrame()

        # Para cada ano, processamos os dados e os consolidamos
        for ano in anos_disponiveis:
            # Filtrar as vendas para o ano e para as lojas disponíveis
            df_ano = df_vendas[(df_vendas['ano'] == ano) & (df_vendas['id_loja'].isin(lojas_disponiveis))].copy()
            
            # Verificar se há dados para este ano
            if df_ano.empty:
                print(f"Não há dados para o ano {ano}")
                continue
            
            # Agrupar por Mês e id_loja, somando o total_venda
            df_mensal = df_ano.groupby(['mes', 'id_loja'])['total_venda'].sum().reset_index()
            
            # Adicionar coluna de Ano ao DataFrame
            df_mensal['ano'] = ano
            
            # Anexar ao DataFrame consolidado
            df_consolidado = pd.concat([df_consolidado, df_mensal], ignore_index=True)

        # Salvar o DataFrame consolidado em um único arquivo Excel
        if not df_consolidado.empty:
            # csv_path = os.path.join(diretorio_cliente, f'{nome_cliente}_faturamento_mensal.csv')
            df_consolidado = df_consolidado.merge(df_lojas[['id_loja', 'nome']], on='id_loja', how='left')
            df_consolidado.rename(columns={'nome': 'nome_loja'}, inplace=True)

            ###############################
            # Salvar relatório
            ###############################
            # df_consolidado.to_csv(csv_path, index=False)
            print(f"Relatório de faturamento por loja/ano gerado para {nome_cliente}")
        else:
            print("Não há dados para gerar o arquivo Excel consolidado.")

        ########################################################
        # Faturamento diário no mês atual por loja
        ########################################################

        # Obter a data atual
        data_atual = pd.Timestamp.now()
        mes_atual = data_atual.month
        ano_atual = data_atual.year
        
        # Lista para armazenar os DataFrames de cada mês
        dfs_loja_meses = []
        
        # Processar mês atual e 3 meses anteriores
        for i in range(4):
            # Calcular o mês e ano de referência
            mes_ref = mes_atual - i
            ano_ref = ano_atual
            
            # Ajustar o ano se necessário (quando o mês for negativo)
            if mes_ref <= 0:
                mes_ref += 12
                ano_ref -= 1
            
            # Filtrar os dados para o mês e ano em questão
            mask = (df_vendas['data_venda'].dt.month == mes_ref) & (df_vendas['data_venda'].dt.year == ano_ref)
            df_mes = df_vendas[mask].copy()
            
            # Se não houver dados, continuar para o próximo mês
            if df_mes.empty:
                print(f"Não há dados para {mes_ref}/{ano_ref}")
                continue
            
            # Criar coluna para dia do mês
            df_mes['dia'] = df_mes['data_venda'].dt.day
            
            # Agrupar por loja e dia, calculando o faturamento diário
            df_diario_loja = df_mes.groupby(['id_loja', 'dia'])['total_venda'].sum().reset_index()
            
            # Mesclar com informações das lojas para obter os nomes
            df_diario_loja = df_diario_loja.merge(df_lojas[['id_loja', 'nome']], on='id_loja', how='left')

            # Renomear coluna nome para melhor clareza
            df_diario_loja.rename(columns={'nome': 'nome_loja'}, inplace=True)
            
            # Adicionar colunas de mês e ano
            df_diario_loja['mes'] = mes_ref
            df_diario_loja['ano'] = ano_ref
            df_diario_loja['periodo'] = f"{mes_ref:02d}/{ano_ref}"
            
            # Adicionar à lista de DataFrames
            dfs_loja_meses.append(df_diario_loja)
        
        # Combinar todos os DataFrames
        if dfs_loja_meses:
            df_faturamento_diario_loja = pd.concat(dfs_loja_meses, ignore_index=True)
            
            # Salvar dados completos em CSV
            ###############################
            # Salvar relatório
            ###############################
            # csv_path = os.path.join(diretorio_cliente, f'{nome_cliente}_faturamento_diario_todas_lojas.csv')
            # df_faturamento_diario_loja.to_csv(csv_path, index=False)
            
            print(f"Relatórios de faturamento diário por loja dos últimos 4 meses gerados para {nome_cliente}")
        else:
            print("Não há dados para gerar análise de faturamento diário por loja nos últimos 4 meses.")
        
        ########################################################
        # Exportar dados para o banco de dados
        ########################################################

        try:
            # Reconectar ao PostgreSQL
            print("\nExportando dados de faturamento para o banco de dados...")
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
            else: 
                print(f"Esquema maloka_analytics já existe no banco {database}.")
            
            # Exportar faturamento anual para o banco de dados
            if 'df_anual_consolidado' in locals() and not df_anual_consolidado.empty:
                # Verificar se a tabela já existe
                cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='faturamento_anual' AND table_schema='maloka_analytics')")
                tabela_existe = cursor.fetchone()[0]
                
                if tabela_existe:
                    # Verificar se as novas colunas existem na tabela
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='faturamento_anual' AND table_schema='maloka_analytics'")
                    colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                    # Adicionar colunas que não existem ainda
                    for coluna in df_anual_consolidado.columns:
                        if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                            print(f"Adicionando nova coluna: {coluna}")
                            
                            # Determinar o tipo de dados da coluna
                            dtype = df_anual_consolidado[coluna].dtype
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
                                cursor.execute(f'ALTER TABLE maloka_analytics.faturamento_anual ADD COLUMN "{coluna}" {tipo}')
                                conn.commit()
                                print(f"Coluna {coluna} adicionada com sucesso!")
                            except Exception as e:
                                print(f"Erro ao adicionar coluna {coluna}: {e}")
                                conn.rollback()
                    
                    # Limpar os dados existentes
                    print("Limpando dados existentes...")
                    cursor.execute("TRUNCATE TABLE maloka_analytics.faturamento_anual")
                    conn.commit()
                else:
                    # Criar a tabela se não existir
                    print("Criando tabela faturamento_anual no esquema maloka_analytics...")
                    # Definir os tipos de dados para cada coluna
                    colunas = []
                    for coluna, dtype in df_anual_consolidado.dtypes.items():
                        if 'int' in str(dtype):
                            tipo = 'INTEGER'
                        elif 'float' in str(dtype):
                            tipo = 'NUMERIC'
                        else:
                            tipo = 'TEXT'
                        colunas.append(f'"{coluna}" {tipo}')
                        
                    # Adicionar coluna para o nome do cliente
                    colunas.append('"cliente" TEXT')
                    
                    create_table_query = f"""
                    CREATE TABLE maloka_analytics.faturamento_anual (
                        {", ".join(colunas)}
                    )
                    """
                    cursor.execute(create_table_query)
                    
                # Adicionar coluna de cliente ao DataFrame
                df_anual_upload = df_anual_consolidado.copy()
                df_anual_upload['cliente'] = nome_cliente

                #quantidade que será inserida
                quantidade_inserir = len(df_anual_upload)
                print(f"Preparando para inserir {quantidade_inserir} registros.")

                # Usar a função para inserir os dados em paralelo
                inserir_dados_paralelo(df_anual_upload, "faturamento_anual", database, nome_cliente, conn, cursor)

            # Exportar faturamento mensal para o banco de dados
            if 'df_consolidado' in locals() and not df_consolidado.empty:
                # Verificar se a tabela já existe
                cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='faturamento_mensal' AND table_schema='maloka_analytics')")
                tabela_existe = cursor.fetchone()[0]
                
                if tabela_existe:
                    # Verificar se as novas colunas existem na tabela
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='faturamento_mensal' AND table_schema='maloka_analytics'")
                    colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                    # Adicionar colunas que não existem ainda
                    for coluna in df_consolidado.columns:
                        if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                            print(f"Adicionando nova coluna: {coluna}")
                            
                            # Determinar o tipo de dados da coluna
                            dtype = df_consolidado[coluna].dtype
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
                                cursor.execute(f'ALTER TABLE maloka_analytics.faturamento_mensal ADD COLUMN "{coluna}" {tipo}')
                                conn.commit()
                                print(f"Coluna {coluna} adicionada com sucesso!")
                            except Exception as e:
                                print(f"Erro ao adicionar coluna {coluna}: {e}")
                                conn.rollback()
                    
                    # Limpar os dados existentes
                    print("Limpando dados existentes...")
                    cursor.execute("TRUNCATE TABLE maloka_analytics.faturamento_mensal")
                    conn.commit()
                else:
                    # Criar a tabela se não existir
                    print("Criando tabela faturamento_mensal no esquema maloka_analytics...")
                    # Definir os tipos de dados para cada coluna
                    colunas = []
                    for coluna, dtype in df_consolidado.dtypes.items():
                        if 'int' in str(dtype):
                            tipo = 'INTEGER'
                        elif 'float' in str(dtype):
                            tipo = 'NUMERIC'
                        elif 'datetime' in str(dtype):
                            tipo = 'TIMESTAMP'
                        elif 'bool' in str(dtype):
                            tipo = 'BOOLEAN'
                        else:
                            # Limitar o tamanho de campos de texto para economizar espaço
                            tipo = 'TEXT'
                        colunas.append(f'"{coluna}" {tipo}')
                    
                    # Adicionar coluna para o nome do cliente
                    colunas.append('"cliente" TEXT')
                    
                    # Criar tabela inicialmente como UNLOGGED para inserção mais rápida
                    create_table_query = f"""
                    CREATE UNLOGGED TABLE maloka_analytics.faturamento_mensal (
                        {", ".join(colunas)}
                    )
                    """
                    cursor.execute(create_table_query)
                    print("Tabela UNLOGGED criada para inserção rápida")
                    
                    # Definir fillfactor para reduzir fragmentação
                    cursor.execute("ALTER TABLE maloka_analytics.faturamento_mensal SET (fillfactor = 90)")
                    
                    # Otimizar configurações temporárias
                    cursor.execute("SET maintenance_work_mem = '256MB'")
                    cursor.execute("SET synchronous_commit = off")
                    
                # Adicionar coluna de cliente ao DataFrame
                df_mensal_upload = df_consolidado.copy()
                df_mensal_upload['cliente'] = nome_cliente

                #quantidade que será inserida
                quantidade_inserir = len(df_mensal_upload)
                print(f"Preparando para inserir {quantidade_inserir} registros.")

                # Usar a função para inserir os dados em paralelo
                inserir_dados_paralelo(df_mensal_upload, "faturamento_mensal", database, nome_cliente, conn, cursor)

            #exportar faturamento diário por loja para o banco de dados
            if 'df_faturamento_diario_loja' in locals() and not df_faturamento_diario_loja.empty:
                # Verificar se a tabela já existe
                cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='faturamento_diario' AND table_schema='maloka_analytics')")
                tabela_existe = cursor.fetchone()[0]

                if tabela_existe:
                    # Verificar se as novas colunas existem na tabela
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='faturamento_diario' AND table_schema='maloka_analytics'")
                    colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                    # Adicionar colunas que não existem ainda
                    for coluna in df_faturamento_diario_loja.columns:
                        if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                            print(f"Adicionando nova coluna: {coluna}")
                            
                            # Determinar o tipo de dados da coluna
                            dtype = df_faturamento_diario_loja[coluna].dtype
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
                                cursor.execute(f'ALTER TABLE maloka_analytics.faturamento_diario ADD COLUMN "{coluna}" {tipo}')
                                conn.commit()
                                print(f"Coluna {coluna} adicionada com sucesso!")
                            except Exception as e:
                                print(f"Erro ao adicionar coluna {coluna}: {e}")
                                conn.rollback()
                    
                    # Limpar os dados existentes
                    print("Limpando dados existentes...")
                    cursor.execute("TRUNCATE TABLE maloka_analytics.faturamento_diario")
                    conn.commit()
                else:
                    # Criar a tabela se não existir
                    print("Criando tabela faturamento_diario no esquema maloka_analytics...")
                    # Definir os tipos de dados para cada coluna
                    colunas = []
                    for coluna, dtype in df_faturamento_diario_loja.dtypes.items():
                        if 'int' in str(dtype):
                            tipo = 'INTEGER'
                        elif 'float' in str(dtype):
                            tipo = 'NUMERIC'
                        elif 'datetime' in str(dtype):
                            tipo = 'TIMESTAMP'
                        elif 'bool' in str(dtype):
                            tipo = 'BOOLEAN'
                        else:
                            # Limitar o tamanho de campos de texto para economizar espaço
                            tipo = 'TEXT'
                        colunas.append(f'"{coluna}" {tipo}')
                    
                    # Adicionar coluna para o nome do cliente
                    colunas.append('"cliente" TEXT')
                    
                    # Criar tabela inicialmente como UNLOGGED para inserção mais rápida
                    create_table_query = f"""
                    CREATE UNLOGGED TABLE maloka_analytics.faturamento_diario (
                        {", ".join(colunas)}
                    )
                    """
                    cursor.execute(create_table_query)
                    print("Tabela UNLOGGED criada para inserção rápida")
                    
                    # Definir fillfactor para reduzir fragmentação
                    cursor.execute("ALTER TABLE maloka_analytics.faturamento_diario SET (fillfactor = 90)")
                    
                    # Otimizar configurações temporárias
                    cursor.execute("SET maintenance_work_mem = '256MB'")
                    cursor.execute("SET synchronous_commit = off")

                # Adicionar coluna de cliente ao DataFrame
                df_diario_upload = df_faturamento_diario_loja.copy()
                df_diario_upload['cliente'] = nome_cliente  

                #quantidade que será inserida
                quantidade_inserir = len(df_diario_upload)
                print(f"Preparando para inserir {quantidade_inserir} registros.")

                # Usar a função para inserir os dados em paralelo
                inserir_dados_paralelo(df_diario_upload, "faturamento_diario", database, nome_cliente, conn, cursor)

            # Fechar cursor e conexão
            cursor.close()
            conn.close()
            print("Exportação de dados para o banco de dados concluída com sucesso!")
            
        except Exception as e:
            print(f"Erro ao exportar dados para o banco de dados: {e}")
            print(traceback.format_exc())
            if 'conn' in locals() and conn is not None:
                conn.close()
                                
    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")
        print("4. O esquema e as tabelas existem para este cliente")

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
            gerar_relatorios_faturamento(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_relatorios_faturamento(args.cliente)



"""
Para executar um cliente específico, use o comando:
python analise_faturamento.py nome_do_cliente

Para executar para todos os clientes, use o comando:
python analise_faturamento.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python analise_faturamento.py
"""