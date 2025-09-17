import traceback
import pandas as pd
import psycopg2
import os
from datetime import datetime
import numpy as np
import warnings
import argparse
import sys
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
    Função para inserir dados em uma tabela usando processamento paralelo para maior eficiência.
    
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

def gerar_analise_recorrencia_retencao(nome_cliente):
    """
    Gera relatórios de vendas atípicas para um cliente específico.
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
    # diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_retencao_recorrencia', nome_cliente)
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
        
        print("Consultando a tabela vendas...")
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
        
        # Exportar para Excel
        #df_vendas.to_excel("df_vendas.xlsx", index=False)

        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")

    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")

    ###############################################################
    #Análise de Recorrência Mensal
    ###############################################################

    # Criar um DataFrame com ano, mês e cliente
    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
    df_vendas['ano'] = df_vendas['data_venda'].dt.year
    df_vendas['mes'] = df_vendas['data_venda'].dt.month
    df_vendas['trimestre'] = (df_vendas['data_venda'].dt.month - 1) // 3 + 1

    monthly_customers = df_vendas.groupby(['ano', 'mes', 'id_cliente']).size().reset_index()
    monthly_customers.columns = ['ano', 'mes', 'id_cliente', 'count']

    # Criar chave composta para o pivot
    monthly_customers['periodo'] = monthly_customers['ano'].astype(str) + '-' + monthly_customers['mes'].astype(str).str.zfill(2)

    # Criar um pivot para facilitar a comparação entre meses
    customer_matrix = monthly_customers.pivot_table(
        index='id_cliente',
        columns='periodo',
        values='count',
        fill_value=0
    ).astype(bool).astype(int)

    # Calcular retenção
    retention_rates = []
    periods = sorted(customer_matrix.columns)

    for i in range(1, len(periods)):
        prev_period = periods[i-1]
        current_period = periods[i]
        
        # Extrair ano e mês do período atual
        current_ano = int(current_period.split('-')[0])
        current_mes = int(current_period.split('-')[1])
        
        # Total de clientes no mês anterior
        prev_customers = customer_matrix[prev_period].sum()
        
        # Clientes que permaneceram
        retained = ((customer_matrix[prev_period] == 1) & (customer_matrix[current_period] == 1)).sum()
        
        # Calcular taxa
        retention_rate = (retained / prev_customers * 100) if prev_customers > 0 else 0
        
        retention_rates.append({
            'ano': current_ano,
            'mes': current_mes,
            'clientes_retidos': retained,
            'total_clientes_mes_anterior': prev_customers,
            'taxa_retencao': retention_rate
        })

    retention_metrics = pd.DataFrame(retention_rates)

    ###############################
    # Salvar relatório
    ###############################
    # Salvar resultados
    # retention_metrics.to_csv(os.path.join(diretorio_cliente, 'metricas_recorrencia_mensal.csv'), index=False)
    print(f"\nResultados de retenção mensal salvos para cliente {nome_cliente}")

    # Estatísticas resumidas
    print("\nEstatísticas de Recorrência:")
    print(f"Taxa média de retenção: {retention_metrics['taxa_retencao'].mean():.1f}%")
    print(f"Taxa máxima de retenção: {retention_metrics['taxa_retencao'].max():.1f}%")
    print(f"Taxa mínima de retenção: {retention_metrics['taxa_retencao'].min():.1f}%")

    ###############################################################
    # Análise de Recorrência Trimestral
    ###############################################################

    # Criar DataFrame de clientes por trimestre
    quarterly_customers = df_vendas.groupby(['ano', 'trimestre', 'id_cliente']).size().reset_index()
    quarterly_customers.columns = ['ano', 'trimestre', 'id_cliente', 'count']

    # Criar chave composta para o pivot
    quarterly_customers['periodo'] = quarterly_customers['ano'].astype(str) + '-' + quarterly_customers['trimestre'].astype(str)

    # Criar pivot para análise trimestral
    customer_matrix = quarterly_customers.pivot_table(
        index='id_cliente',
        columns='periodo',
        values='count',
        fill_value=0
    ).astype(bool).astype(int)

    # Calcular recorrência trimestral
    quarterly_metrics = []
    periods = sorted(customer_matrix.columns)

    for i in range(1, len(periods)):
        current_period = periods[i]
        prev_period = periods[i-1]
        
        # Extrair ano e trimestre
        current_ano = int(current_period.split('-')[0])
        current_trimestre = int(current_period.split('-')[1])
        
        # Total de clientes no trimestre anterior
        prev_customers = customer_matrix[prev_period].sum()
        
        # Clientes que voltaram
        returning = ((customer_matrix[prev_period] == 1) & (customer_matrix[current_period] == 1)).sum()
        
        # Novos clientes (não compraram no trimestre anterior)
        new_customers = (customer_matrix[current_period] & ~customer_matrix[prev_period]).sum()
        
        # Total de clientes no trimestre atual
        total_customers = customer_matrix[current_period].sum()
        
        # Calcular taxas
        recurrence_rate = (returning / prev_customers * 100) if prev_customers > 0 else 0
        
        quarterly_metrics.append({
            'ano': current_ano,
            'trimestre': current_trimestre,
            'total_clientes': total_customers,
            'clietes_retornantes': returning,
            'clientes_novos': new_customers,
            'taxa_recorrencia': recurrence_rate
        })

    quarterly_df = pd.DataFrame(quarterly_metrics)

    # oredenar por ano e trimeste
    quarterly_df = quarterly_df.sort_values(['ano', 'trimestre'])

    ###############################
    # Salvar relatório
    ###############################
    # Salvar resultados
    # quarterly_df.to_csv(os.path.join(diretorio_cliente, 'metricas_recorrencia_trimestral.csv'), index=False)
    print(f"\nResultados recorrência trimestral salvos para cliente {nome_cliente}")

    # Estatísticas resumidas
    print("\nEstatísticas de Recorrência Trimestral:")
    print(f"Taxa média de recorrência: {quarterly_df['taxa_recorrencia'].mean():.1f}%")
    print(f"Taxa máxima de recorrência: {quarterly_df['taxa_recorrencia'].max():.1f}%")
    print(f"Taxa mínima de recorrência: {quarterly_df['taxa_recorrencia'].min():.1f}%")
    print(f"\nMédia de novos clientes por trimestre: {quarterly_df['clientes_novos'].mean():.0f}")
    print(f"Média de clientes recorrentes por trimestre: {quarterly_df['clietes_retornantes'].mean():.0f}")

    ###############################################################
    #Análise de Recorrência Anual
    ###############################################################

    # Criar ano para cada compra
    df_vendas['ano'] = df_vendas['data_venda'].dt.to_period('Y')

    # Criar DataFrame de clientes por ano
    annual_customers = df_vendas.groupby(['ano', 'id_cliente']).size().reset_index()
    annual_customers.columns = ['ano', 'id_cliente', 'count']

    # Criar pivot para análise anual
    customer_matrix = annual_customers.pivot_table(
        index='id_cliente',
        columns='ano',
        values='count',
        fill_value=0
    ).astype(bool).astype(int)

    # Calcular métricas anuais
    annual_metrics = []
    years = sorted(customer_matrix.columns)

    for i in range(1, len(years)):
        current_year = years[i]
        prev_year = years[i-1]
        
        # Total de clientes no ano anterior
        prev_customers = customer_matrix[prev_year].sum()
        
        # Clientes que voltaram
        returning = ((customer_matrix[prev_year] == 1) & (customer_matrix[current_year] == 1)).sum()
        
        # Novos clientes (não compraram no ano anterior)
        new_customers = (customer_matrix[current_year] & ~customer_matrix[prev_year]).sum()
        
        # Total de clientes no ano atual
        total_customers = customer_matrix[current_year].sum()
        
        # Calcular taxas
        retention_rate = (returning / prev_customers * 100) if prev_customers > 0 else 0
        new_rate = (new_customers / total_customers * 100) if total_customers > 0 else 0
        returning_rate = (returning / total_customers * 100) if total_customers > 0 else 0
        
        annual_metrics.append({
            'ano': str(current_year),
            'ano_obj': current_year,
            'total_clientes': total_customers,
            'clietes_retornantes': returning,
            'clientes_novos': new_customers,
            'taxa_retencao': retention_rate,  # Taxa de retenção em relação ao ano anterior
            'taxa_clientes_novos': new_rate,  # % de novos clientes no total
            'taxa_clientes_retornantes': returning_rate  # % de clientes recorrentes no total
        })

    annual_df = pd.DataFrame(annual_metrics)
    annual_df = annual_df.sort_values('ano_obj')

    #remover a coluna 'ano_obj' para melhor visualização
    annual_df = annual_df.drop(columns=['ano_obj'])

    ###############################
    # Salvar relatório
    ###############################
    # Salvar resultados
    # annual_df.to_csv(os.path.join(diretorio_cliente, 'metricas_recorrencia_anual.csv'), index=False)
    print(f"\nResultados metricas recorrência anual salvos para cliente {nome_cliente}")

    print("\nEstatísticas de Recorrência Anual:")
    print(f"Taxa média de retenção: {annual_df['taxa_retencao'].mean():.1f}%")
    print(f"Taxa média de novos clientes: {annual_df['taxa_clientes_novos'].mean():.1f}%")
    print(f"Taxa média de recorrentes: {annual_df['taxa_clientes_retornantes'].mean():.1f}%")


    ###############################################################
    #Análise de Retenção Anual
    ###############################################################

    # ---- Análise de Cohort: Agrupando Clientes em Coortes Anuais com Base na Primeira Compra ----

    # Utilizaremos a base de dados df_vendas que já possui a coluna 'data_venda' no formato datetime.
    # Cada pedido será associado a um período anual, e cada cliente receberá a coorte (ano) de sua primeira compra.

    # 1. Definir o período do pedido como ano
    df_vendas['order_period'] = df_vendas['data_venda'].dt.to_period('Y')

    # 2. Para cada cliente, identificar a data da primeira compra e definir sua coorte anual
    first_purchase = df_vendas.groupby('id_cliente')['data_venda'].min().reset_index()
    first_purchase.columns = ['id_cliente', 'first_purchase_date']
    first_purchase['ano_analise'] = first_purchase['first_purchase_date'].dt.to_period('Y')

    # 3. Mesclar a informação da coorte (ano da primeira compra) de volta à base de pedidos
    df_vendas = pd.merge(df_vendas, first_purchase[['id_cliente', 'ano_analise']], on='id_cliente')

    # 4. Calcular o índice do período (em anos) para cada pedido: quantos anos se passaram desde a coorte
    df_vendas['periodo_ano'] = (df_vendas['order_period'] - df_vendas['ano_analise']).apply(lambda x: x.n)

    # 5. Agregar os dados para contar o número de clientes únicos por coorte e por período
    cohort_data = df_vendas.groupby(['ano_analise', 'periodo_ano'])['id_cliente'].nunique().reset_index()
    cohort_data.rename(columns={'id_cliente': 'qtd_clientes'}, inplace=True)

    # 6. Obter o tamanho inicial de cada coorte (ou seja, o número de clientes no período 0)
    cohort_sizes = cohort_data[cohort_data['periodo_ano'] == 0][['ano_analise', 'qtd_clientes']]
    cohort_sizes.rename(columns={'qtd_clientes': 'qtd_clientes_ano_analise'}, inplace=True)

    # 7. Mesclar o tamanho da coorte com os dados agregados
    cohort_data = pd.merge(cohort_data, cohort_sizes, on='ano_analise')

    # 8. Calcular a taxa de retenção para cada coorte e período:
    #Taxa de Retenção = Número de clientes ativos no período / Tamanho inicial da coorte
    cohort_data['taxa_retencao'] = cohort_data['qtd_clientes'] / cohort_data['qtd_clientes_ano_analise']

    ###############################
    # Salvar relatório
    ###############################
    # Exportar os resultados para um arquivo CSV
    # cohort_data.to_csv(os.path.join(diretorio_cliente, 'metricas_retencao_anual.csv'), index=False)
    print(f"\nResultados métricas retenção anual salvos para cliente {nome_cliente}")

    ########################################################
    # Gravação em banco
    ########################################################
    
    try:
        # Reconectar ao PostgreSQL
        print("\nExportando dados de retenção e recorrência para o banco de dados...")
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
        
        # 1. Exportar métricas de recorrência mensal
        if 'retention_metrics' in locals() and not retention_metrics.empty:
            # Verificar se a tabela já existe
            cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='recorrencia_mensal' AND table_schema='maloka_analytics')")
            tabela_existe = cursor.fetchone()[0]
            
            if tabela_existe:
                # Verificar se as novas colunas existem na tabela
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='recorrencia_mensal' AND table_schema='maloka_analytics'")
                colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                # Adicionar colunas que não existem ainda
                for coluna in retention_metrics.columns:
                    if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                        print(f"Adicionando nova coluna: {coluna}")
                            
                        # Determinar o tipo de dados da coluna
                        dtype = retention_metrics[coluna].dtype
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
                            cursor.execute(f'ALTER TABLE maloka_analytics.recorrencia_mensal ADD COLUMN "{coluna}" {tipo}')
                            conn.commit()
                            print(f"Coluna {coluna} adicionada com sucesso!")
                        except Exception as e:
                            print(f"Erro ao adicionar coluna {coluna}: {e}")
                            conn.rollback()
                    
                # Limpar os dados existentes
                print("Limpando dados existentes...")
                cursor.execute("TRUNCATE TABLE maloka_analytics.recorrencia_mensal")
                conn.commit()
            else:
                # Criar a tabela se não existir
                print("Criando tabela recorrencia_mensal no esquema maloka_analytics...")
                create_table_query = """
                CREATE UNLOGGED TABLE maloka_analytics.recorrencia_mensal (
                    id SERIAL PRIMARY KEY,
                    ano INTEGER,
                    mes INTEGER,
                    clientes_retidos INTEGER,
                    total_clientes_mes_anterior INTEGER,
                    taxa_retencao DECIMAL,
                    cliente TEXT,
                    CONSTRAINT recorrencia_mensal_unique UNIQUE (ano, mes, cliente)
                )
                """
                cursor.execute(create_table_query)
                print("Tabela UNLOGGED criada para inserção rápida")
                    
                # Definir fillfactor para reduzir fragmentação
                cursor.execute("ALTER TABLE maloka_analytics.recorrencia_mensal SET (fillfactor = 90)")
                    
                # Otimizar configurações temporárias
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET synchronous_commit = off")
                
            # Adicionar coluna de cliente ao DataFrame
            df_mensal_upload = retention_metrics.copy()
            df_mensal_upload['cliente'] = nome_cliente

            # Quantidade que será inserida
            quantidade_inserir = len(df_mensal_upload)
            print(f"Preparando para inserir {quantidade_inserir} registros de recorrência mensal.")
            
            # Usar a função para inserir os dados em paralelo
            inserir_dados_paralelo(df_mensal_upload, "recorrencia_mensal", database, nome_cliente, conn, cursor)
        
        # 2. Exportar métricas de recorrência trimestral
        if 'quarterly_df' in locals() and not quarterly_df.empty:
            # Verificar se a tabela já existe
            cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='recorrencia_trimestral' AND table_schema='maloka_analytics')")
            tabela_existe = cursor.fetchone()[0]
            
            if tabela_existe:
                # Verificar se as novas colunas existem na tabela
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='recorrencia_trimestral' AND table_schema='maloka_analytics'")
                colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                # Adicionar colunas que não existem ainda
                for coluna in quarterly_df.columns:
                    if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                        print(f"Adicionando nova coluna: {coluna}")
                            
                        # Determinar o tipo de dados da coluna
                        dtype = quarterly_df[coluna].dtype
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
                            cursor.execute(f'ALTER TABLE maloka_analytics.recorrencia_trimestral ADD COLUMN "{coluna}" {tipo}')
                            conn.commit()
                            print(f"Coluna {coluna} adicionada com sucesso!")
                        except Exception as e:
                            print(f"Erro ao adicionar coluna {coluna}: {e}")
                            conn.rollback()
                    
                # Limpar os dados existentes
                print("Limpando dados existentes...")
                cursor.execute("TRUNCATE TABLE maloka_analytics.recorrencia_trimestral")
                conn.commit()
            else:
                # Criar a tabela com a nova estrutura
                print("Criando tabela recorrencia_trimestral no esquema maloka_analytics...")
                create_table_query = """
                CREATE UNLOGGED TABLE maloka_analytics.recorrencia_trimestral (
                    id SERIAL PRIMARY KEY,
                    ano INTEGER,
                    trimestre INTEGER,
                    total_clientes INTEGER,
                    clietes_retornantes INTEGER,
                    clientes_novos INTEGER, 
                    taxa_recorrencia DECIMAL,
                    cliente TEXT,
                    CONSTRAINT recorrencia_trimestral_unique UNIQUE (ano, trimestre, cliente)
                )
                """
                cursor.execute(create_table_query)
                print("Tabela UNLOGGED criada para inserção rápida")
                    
                # Definir fillfactor para reduzir fragmentação
                cursor.execute("ALTER TABLE maloka_analytics.recorrencia_trimestral SET (fillfactor = 90)")
                    
                # Otimizar configurações temporárias
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET synchronous_commit = off")
                
            # Adicionar coluna de cliente ao DataFrame
            df_trimestral_upload = quarterly_df.copy()
            df_trimestral_upload['cliente'] = nome_cliente

            # Quantidade que será inserida
            quantidade_inserir = len(df_trimestral_upload)
            print(f"Preparando para inserir {quantidade_inserir} registros de recorrência trimestral.")
            
            # Usar a função para inserir os dados em paralelo
            inserir_dados_paralelo(df_trimestral_upload, "recorrencia_trimestral", database, nome_cliente, conn, cursor)
        
        # 3. Exportar métricas de recorrência anual
        if 'annual_df' in locals() and not annual_df.empty:
            # Verificar se a tabela já existe
            cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='recorrencia_anual' AND table_schema='maloka_analytics')")
            tabela_existe = cursor.fetchone()[0]
            
            if tabela_existe:
                # Verificar se as novas colunas existem na tabela
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='recorrencia_anual' AND table_schema='maloka_analytics'")
                colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                # Adicionar colunas que não existem ainda
                for coluna in annual_df.columns:
                    if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                        print(f"Adicionando nova coluna: {coluna}")
                            
                        # Determinar o tipo de dados da coluna
                        dtype = annual_df[coluna].dtype
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
                            cursor.execute(f'ALTER TABLE maloka_analytics.recorrencia_anual ADD COLUMN "{coluna}" {tipo}')
                            conn.commit()
                            print(f"Coluna {coluna} adicionada com sucesso!")
                        except Exception as e:
                            print(f"Erro ao adicionar coluna {coluna}: {e}")
                            conn.rollback()
                    
                # Limpar os dados existentes
                print("Limpando dados existentes...")
                cursor.execute("TRUNCATE TABLE maloka_analytics.recorrencia_anual")
                conn.commit()
            else:
                # Criar a tabela se não existir
                print("Criando tabela recorrencia_anual no esquema maloka_analytics...")
                create_table_query = """
                CREATE UNLOGGED TABLE maloka_analytics.recorrencia_anual (
                    id SERIAL PRIMARY KEY,
                    ano TEXT,
                    total_clientes INTEGER,
                    clietes_retornantes INTEGER,
                    clientes_novos INTEGER,
                    taxa_retencao DECIMAL,
                    taxa_clientes_novos DECIMAL,
                    taxa_clientes_retornantes DECIMAL,
                    cliente TEXT,
                    CONSTRAINT recorrencia_anual_unique UNIQUE (ano, cliente)
                )
                """
                cursor.execute(create_table_query)
                print("Tabela UNLOGGED criada para inserção rápida")
                    
                # Definir fillfactor para reduzir fragmentação
                cursor.execute("ALTER TABLE maloka_analytics.recorrencia_anual SET (fillfactor = 90)")
                    
                # Otimizar configurações temporárias
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET synchronous_commit = off")
                
            # Adicionar coluna de cliente ao DataFrame
            df_anual_upload = annual_df.copy()
            df_anual_upload['cliente'] = nome_cliente

            # Quantidade que será inserida
            quantidade_inserir = len(df_anual_upload)
            print(f"Preparando para inserir {quantidade_inserir} registros de recorrência anual.")
            
            # Usar a função para inserir os dados em paralelo
            inserir_dados_paralelo(df_anual_upload, "recorrencia_anual", database, nome_cliente, conn, cursor)

        # 4. Exportar métricas de retenção anual (análise de coortes)
        if 'cohort_data' in locals() and not cohort_data.empty:
            # Verificar se a tabela já existe
            cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='retencao_anual' AND table_schema='maloka_analytics')")
            tabela_existe = cursor.fetchone()[0]
            
            if tabela_existe:
                # Verificar se as novas colunas existem na tabela
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='retencao_anual' AND table_schema='maloka_analytics'")
                colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                # Adicionar colunas que não existem ainda
                for coluna in cohort_data.columns:
                    if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                        print(f"Adicionando nova coluna: {coluna}")
                            
                        # Determinar o tipo de dados da coluna
                        dtype = cohort_data[coluna].dtype
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
                            cursor.execute(f'ALTER TABLE maloka_analytics.retencao_anual ADD COLUMN "{coluna}" {tipo}')
                            conn.commit()
                            print(f"Coluna {coluna} adicionada com sucesso!")
                        except Exception as e:
                            print(f"Erro ao adicionar coluna {coluna}: {e}")
                            conn.rollback()
                    
                # Limpar os dados existentes
                print("Limpando dados existentes...")
                cursor.execute("TRUNCATE TABLE maloka_analytics.retencao_anual")
                conn.commit()
            else:
                # Criar a tabela se não existir
                print("Criando tabela retencao_anual no esquema maloka_analytics...")
                create_table_query = """
                CREATE UNLOGGED TABLE maloka_analytics.retencao_anual (
                    id SERIAL PRIMARY KEY,
                    ano_analise TEXT,
                    periodo_ano INTEGER,
                    qtd_clientes INTEGER,
                    qtd_clientes_ano_analise INTEGER,
                    taxa_retencao DECIMAL,
                    cliente TEXT,
                    CONSTRAINT retencao_anual_unique UNIQUE (ano_analise, periodo_ano, cliente)
                )
                """
                cursor.execute(create_table_query)
                print("Tabela UNLOGGED criada para inserção rápida")
                    
                # Definir fillfactor para reduzir fragmentação
                cursor.execute("ALTER TABLE maloka_analytics.retencao_anual SET (fillfactor = 90)")
                    
                # Otimizar configurações temporárias
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET synchronous_commit = off")
                
            # Adicionar coluna de cliente ao DataFrame
            df_retencao_upload = cohort_data.copy()
            df_retencao_upload['cliente'] = nome_cliente
            
            # Converter objetos Period para string se necessário
            if 'ano_analise' in df_retencao_upload.columns and hasattr(df_retencao_upload['ano_analise'], 'dt'):
                df_retencao_upload['ano_analise'] = df_retencao_upload['ano_analise'].astype(str)
                
            # Quantidade que será inserida
            quantidade_inserir = len(df_retencao_upload)
            print(f"Preparando para inserir {quantidade_inserir} registros de retenção anual.")
            
            # Usar a função para inserir os dados em paralelo
            inserir_dados_paralelo(df_retencao_upload, "retencao_anual", database, nome_cliente, conn, cursor)
            
        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        print("Exportação de dados para o banco de dados concluída com sucesso!")
        
    except Exception as e:
        print(f"Erro ao exportar dados para o banco de dados: {e}")
        print(traceback.format_exc())
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
            gerar_analise_recorrencia_retencao(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_analise_recorrencia_retencao(args.cliente)

"""
Para executar um cliente específico, use o comando:
python analise_segmentacao.py nome_do_cliente

Para executar para todos os clientes, use o comando:
python analise_segmentacao.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python analise_segmentacao.py
"""
