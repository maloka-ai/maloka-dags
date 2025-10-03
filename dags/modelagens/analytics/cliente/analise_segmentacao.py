import pandas as pd
import psycopg2
import os
import numpy as np
import warnings
from datetime import datetime
import argparse
import unicodedata
import sys
from multiprocessing import Pool, cpu_count
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

def inserir_dados_paralelo(df, tabela, database, conn, cursor, schema="maloka_analytics"):
    """
    Função para inserir dados em uma tabela usando processamento paralelo para maior eficiência.
    
    Args:
        df: DataFrame com os dados a serem inseridos
        tabela: Nome da tabela no banco de dados (sem o esquema)
        database: Nome do banco de dados
        conn: Conexão com o banco de dados
        cursor: Cursor para execução de comandos SQL
        schema: Nome do esquema onde a tabela está (padrão: "maloka_analytics")
        
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
            
        # Preparar a query com ON CONFLICT para lidar com chaves duplicadas
        placeholders = ", ".join(["%s"] * len(df_upload.columns))
        
        # Se tabela for segmentacao, verificar se ela tem restrição de chave primária para id_cliente
        if tabela == "segmentacao":
            # Verificar se existe uma chave primária ou restrição única para id_cliente
            conn_check = psycopg2.connect(
                host=DB_CONFIG_MALOKA['host'],
                database=database,
                user=DB_CONFIG_MALOKA['user'],
                password=DB_CONFIG_MALOKA['password'],
                port=DB_CONFIG_MALOKA['port']
            )
            cursor_check = conn_check.cursor()
            
            # Verificar se existe primary key ou unique constraint na coluna id_cliente
            cursor_check.execute(f"""
                SELECT COUNT(*) FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                AND tc.table_schema = '{schema}'
                AND tc.table_name = '{tabela}'
                AND ccu.column_name = 'id_cliente'
            """)
            
            has_constraint = cursor_check.fetchone()[0] > 0
            cursor_check.close()
            conn_check.close()
            
            if has_constraint:
                # Obter todas as colunas exceto id_cliente
                update_cols = [col for col in df_upload.columns if col != "id_cliente"]
                update_stmts = [f'"{col}" = EXCLUDED."{col}"' for col in update_cols]
                
                insert_query = f"""
                INSERT INTO {schema}.{tabela} ({", ".join(colunas)})
                VALUES ({placeholders})
                ON CONFLICT (id_cliente) DO UPDATE SET
                {", ".join(update_stmts)}
                """
            else:
                print(f"Aviso: A tabela {schema}.{tabela} não possui restrição única na coluna id_cliente.")
                print("Usando INSERT simples sem ON CONFLICT.")
                insert_query = f"""
                INSERT INTO {schema}.{tabela} ({", ".join(colunas)})
                VALUES ({placeholders})
                """
        else:
            insert_query = f"""
            INSERT INTO {schema}.{tabela} ({", ".join(colunas)})
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
                
            # Adicionar lote à lista de tarefas (incluindo o schema nas instruções SQL)
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
                cursor.execute(f"ALTER TABLE {schema}.{tabela} SET LOGGED")
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
                cursor.execute(f"ALTER TABLE {schema}.{tabela} SET LOGGED")
                    
                # Analisar tabela para otimizar planejamento de consultas
                print(f"Analisando tabela {tabela} para otimizar consultas...")
                cursor.execute(f"ANALYZE {schema}.{tabela}")
                    
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
            
            # Se tabela for segmentacao, verificar se ela tem restrição de chave primária para id_cliente
            if tabela == "segmentacao":
                # Verificar se existe uma chave primária ou restrição única para id_cliente
                cursor_check = conn.cursor()
                
                # Verificar se existe primary key ou unique constraint na coluna id_cliente
                cursor_check.execute(f"""
                    SELECT COUNT(*) FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu 
                    ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                    AND tc.table_schema = '{schema}'
                    AND tc.table_name = '{tabela}'
                    AND ccu.column_name = 'id_cliente'
                """)
                
                has_constraint = cursor_check.fetchone()[0] > 0
                
                if has_constraint:
                    # Obter todas as colunas exceto id_cliente
                    update_cols = [col for col in df_upload.columns if col != "id_cliente"]
                    update_stmts = [f'"{col}" = EXCLUDED."{col}"' for col in update_cols]
                    
                    insert_query = f"""
                    INSERT INTO {schema}.{tabela} ({", ".join(colunas)})
                    VALUES ({placeholders})
                    ON CONFLICT (id_cliente) DO UPDATE SET
                    {", ".join(update_stmts)}
                    """
                else:
                    print(f"Aviso: A tabela {schema}.{tabela} não possui restrição única na coluna id_cliente.")
                    print("Usando INSERT simples sem ON CONFLICT.")
                    insert_query = f"""
                    INSERT INTO {schema}.{tabela} ({", ".join(colunas)})
                    VALUES ({placeholders})
                    """
            else:
                insert_query = f"""
                INSERT INTO {schema}.{tabela} ({", ".join(colunas)})
                VALUES ({placeholders})
                """
            
            valores = [tuple(row) for _, row in df_upload.iterrows()]
            cursor.executemany(insert_query, valores)
            conn.commit()
            
            # Restaurar configurações
            try:
                cursor.execute("SET maintenance_work_mem = '64MB'")  # Valor padrão
                cursor.execute("SET synchronous_commit = on")  # Valor padrão
                cursor.execute(f"ALTER TABLE {schema}.{tabela} SET LOGGED")
                cursor.execute(f"ANALYZE {schema}.{tabela}")
            except Exception as e:
                print(f"Aviso ao restaurar configurações: {e}")
            
            print(f"Dados de {tabela} inseridos com sucesso usando método tradicional! Total de {len(df_upload)} registros.")
            return True
        except Exception as e2:
            print(f"Erro também no método tradicional: {e2}")
            return False

def gerar_analise_cliente(nome_cliente, data_referencia=None):
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
    separar_pf_pj = config_cliente.get("separar_pf_pj", False)
    recencia_recentes = config_cliente["recencia_recentes"]
    recencia_fieis = config_cliente["recencia_fieis"]
    recencia_campeoes = config_cliente["recencia_campeoes"]
    frequencia_recentes = config_cliente["frequencia_recentes"]
    frequencia_fieis = config_cliente["frequencia_fieis"]
    frequencia_campeoes = config_cliente["frequencia_campeoes"]
    idade_fieis = config_cliente["idade_fieis"]
    sumidos = config_cliente["sumidos"]
    inatividade = config_cliente["inatividade"]
    vm_decil_recentes = config_cliente["decil_monetario_recentes"]
    vm_decil_fieis = config_cliente["decil_monetario_fieis"]
    vm_decil_campeoes = config_cliente["decil_monetario_campeoes"]
    descricao = config_cliente["descricao_regras"]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    
    # Criar diretório para salvar os relatórios do cliente
    # diretorio_cliente = os.path.join(diretorio_atual, 'relatorio_segmentacao', nome_cliente)
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
        # Consulta vendas com filtro de data
        if data_referencia:
            query = f"SELECT * FROM {schema}.venda WHERE data_venda <= '{data_referencia}'"
        else:
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
        # print(f"Colunas disponíveis: {', '.join(df_vendas.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_vendas.head())

        ########################################################
        # consulta da tabela clientes
        ########################################################
        
        # Consultar a tabela clientes
        print("Consultando a tabela clientes...")
        query = f"SELECT * FROM {schema}.cliente"
        
        # Carregar os dados diretamente em um DataFrame do pandas
        df_clientes = pd.read_sql_query(query, conn)
        
        # Informações sobre os dados
        num_registros = len(df_clientes)
        num_colunas = len(df_clientes.columns)
        
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_clientes.columns)}")
        
        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_clientes.head())

        ########################################################
        # consulta da tabela cliente pessoa física e jurídica
        ########################################################
        print("Consultando a tabela cliente pessoa física...")
        query = f"SELECT * FROM {schema}.cliente_pessoa_fisica"
        df_clientes_PF = pd.read_sql_query(query, conn)
        
        print("Consultando a tabela cliente pessoa jurídica...")
        query = f"SELECT * FROM {schema}.cliente_pessoa_juridica"
        df_clientes_PJ = pd.read_sql_query(query, conn)

        ########################################################
        # pegar a segementacao atual de clientes 
        ########################################################
        print("Consultando a tabela de segmentação de clientes...") 
        query ="""
        SELECT id_cliente, segmento, penultima_segmentacao
        FROM maloka_analytics.segmentacao
        """
        df_segmentacao_atual = pd.read_sql_query(query, conn)
        num_registros = len(df_segmentacao_atual)
        num_colunas = len(df_segmentacao_atual.columns)
        print(f"Dados obtidos com sucesso! {num_registros} registros e {num_colunas} colunas.")
        # print(f"Colunas disponíveis: {', '.join(df_segmentacao_atual.columns)}")

        # Exibir uma amostra dos dados
        # print("\nPrimeiros 5 registros para verificação:")
        # print(df_segmentacao_atual.head())

        # Fechar conexão
        conn.close()
        print("\nConexão com o banco de dados fechada.")

    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")
        return

    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])

    #filtrar df_vendas para somente vendas que possuem a tipo_venda como PEDIDO e situacao_venda como CONCLUIDA
    df_vendas = df_vendas[(df_vendas['tipo_venda'] == 'PEDIDO') & (df_vendas['situacao_venda'] == 'CONCLUIDA')]

    # MODIFICAÇÃO: Separar os dataframes de vendas se necessário
    if separar_pf_pj:
        print("Separando análises para pessoa física e jurídica...")
        
        if 'tipo' not in df_clientes.columns:
            print("AVISO: A coluna 'tipo' não existe no dataframe de clientes.")
            print("A separação PF/PJ será feita através das tabelas cliente_pessoa_fisica e cliente_pessoa_juridica.")
            
            clientes_pf = df_clientes_PF['id_cliente'].unique()
            clientes_pj = df_clientes_PJ['id_cliente'].unique()
            
            df_vendas_pf = df_vendas[df_vendas['id_cliente'].isin(clientes_pf)]
            df_vendas_pj = df_vendas[df_vendas['id_cliente'].isin(clientes_pj)]
        else:
            print("Separando vendas por tipo (F/J) usando a coluna 'tipo' da tabela clientes")
            # Criar uma lista de clientes PF e PJ baseada na coluna 'tipo' da tabela clientes
            clientes_pf = df_clientes[df_clientes['tipo'] == 'F']['id_cliente'].unique()
            clientes_pj = df_clientes[df_clientes['tipo'] == 'J']['id_cliente'].unique()
            
            # Filtrar as vendas com base nessas listas
            df_vendas_pf = df_vendas[df_vendas['id_cliente'].isin(clientes_pf)]
            df_vendas_pj = df_vendas[df_vendas['id_cliente'].isin(clientes_pj)]

        # Adicionar contagem de clientes e vendas
        print(f"Clientes PF: {len(clientes_pf)}, Clientes PJ: {len(clientes_pj)}")
        print(f"Vendas PF: {len(df_vendas_pf)}, Vendas PJ: {len(df_vendas_pj)}")
        
        # Calcular clientes únicos com vendas
        clientes_com_vendas_pf = df_vendas_pf['id_cliente'].nunique()
        clientes_com_vendas_pj = df_vendas_pj['id_cliente'].nunique()
        print(f"Clientes únicos com vendas - PF: {clientes_com_vendas_pf}, PJ: {clientes_com_vendas_pj}")
        
        # Processar cada conjunto de dados separadamente
        resultados = []
        
        # Loop para processar PF e PJ
        for tipo, df_vendas_tipo, df_clientes_tipo, tipo_nome in [
            ('PF', df_vendas_pf, df_clientes_PF, 'Pessoa Física'),
            ('PJ', df_vendas_pj, df_clientes_PJ, 'Pessoa Jurídica')
        ]:
            print(f"\n{'='*50}")
            print(f"Processando {tipo_nome}")
            print(f"{'='*50}")
            
            ##########################################################
            # Calcular as métricas RFMA para o tipo específico
            ##########################################################
            
            # Recency: Número de dias desde a última compra
            if data_referencia:
                data_referencia = pd.to_datetime(data_referencia)
                print(f"Usando data de referência específica: {data_referencia.strftime('%Y-%m-%d')}")
            else:
                data_referencia = df_vendas['data_venda'].max()
                print(f"Usando data de referência atual: {data_referencia.strftime('%Y-%m-%d')}")
            recency = df_vendas_tipo.groupby('id_cliente')['data_venda'].max().apply(lambda x: (data_referencia - x).days)
            
            # Frequency: Número de pedidos únicos por cliente
            frequency = df_vendas_tipo.groupby('id_cliente')['id_venda'].nunique()
            
            # Monetary: Valor total gasto
            monetary = df_vendas_tipo.groupby('id_cliente')['total_venda'].sum()
            
            # Age (Antiguidade): Dias desde a primeira compra
            age = df_vendas_tipo.groupby('id_cliente')['data_venda'].min().apply(lambda x: (data_referencia - x).days)
            
            # Combinar métricas em um único DataFrame
            rfma = pd.DataFrame({
                'recencia': recency,
                'frequencia': frequency,
                'valor_monetario': monetary,
                'antiguidade': age
            })
            
            # Resetar o índice para ter id_cliente como coluna
            rfma = rfma.reset_index()
            
            # Adicionar algumas verificações
            print(f"\nEstatísticas das métricas RFMA para {tipo_nome}:")
            print(f"\nTotal de clientes: {len(rfma)}")
            print("\nRecency (dias):")
            print(rfma['recencia'].describe())
            print("\nFrequency (número de pedidos):")
            print(rfma['frequencia'].describe())
            print("\nMonetary (valor total):")
            print(rfma['valor_monetario'].describe())
            
            ##############################################################
            # RFMA - Análise de clientes por Decis
            ###############################################################
            
            # Converter as colunas para float (caso estejam em decimal.Decimal)
            for col in ['recencia', 'frequencia', 'valor_monetario', 'antiguidade']:
                rfma[col] = rfma[col].astype(float)
            
            # Criar decis para cada métrica RFMA
            # Primeiro vamos obter os bins para cada métrica
            try:
                _, r_bins = pd.qcut(rfma['recencia'], q=10, duplicates='drop', retbins=True)
                _, f_bins = pd.qcut(rfma['frequencia'], q=10, duplicates='drop', retbins=True)
                _, m_bins = pd.qcut(rfma['valor_monetario'], q=10, duplicates='drop', retbins=True)
                _, a_bins = pd.qcut(rfma['antiguidade'], q=10, duplicates='drop', retbins=True)
                
                # Agora criar os labels com o número correto de categorias
                r_labels = list(range(len(r_bins)-1, 0, -1))  # Ordem inversa para Recency
                f_labels = list(range(1, len(f_bins)))  # Ordem normal para os demais
                m_labels = list(range(1, len(m_bins)))
                a_labels = list(range(1, len(a_bins)))
                
                # Aplicar os decis
                rfma['r_decil'] = pd.qcut(rfma['recencia'], q=10, labels=r_labels, duplicates='drop')
                rfma['f_decil'] = pd.qcut(rfma['frequencia'], q=10, labels=f_labels, duplicates='drop')
                rfma['vm_decil'] = pd.qcut(rfma['valor_monetario'], q=10, labels=m_labels, duplicates='drop')
                rfma['a_decil'] = pd.qcut(rfma['antiguidade'], q=10, labels=a_labels, duplicates='drop')
                
                # Criar ranges para visualização
                rfma['r_range'] = pd.cut(
                    rfma['recencia'], 
                    bins=r_bins, 
                    labels=[f"{r_bins[i]:.1f}-{r_bins[i+1]:.1f}" for i in range(len(r_bins)-1)], 
                    include_lowest=True
                )
                
                rfma['f_range'] = pd.cut(
                    rfma['frequencia'], 
                    bins=f_bins, 
                    labels=[f"{f_bins[i]:.1f}-{f_bins[i+1]:.1f}" for i in range(len(f_bins)-1)], 
                    include_lowest=True
                )
                
                rfma['vm_range'] = pd.cut(
                    rfma['valor_monetario'], 
                    bins=m_bins, 
                    labels=[f"{m_bins[i]:.1f}-{m_bins[i+1]:.1f}" for i in range(len(m_bins)-1)], 
                    include_lowest=True
                )
                
                rfma['a_range'] = pd.cut(
                    rfma['antiguidade'], 
                    bins=a_bins, 
                    labels=[f"{a_bins[i]:.1f}-{a_bins[i+1]:.1f}" for i in range(len(a_bins)-1)], 
                    include_lowest=True
                )
            except Exception as e:
                print(f"AVISO: Não foi possível calcular decis para {tipo_nome}: {e}")
                print("Talvez não existam clientes suficientes para este tipo ou há muitos valores idênticos.")
                continue
                
            ################################################################
            # RFMA - Segmentação baseada em Regras
            ################################################################

            def segment_customers(df):
                """
                Segmenta clientes com base em regras atualizadas, de acordo com a análise da distribuição por faixa.
                
                Parâmetros:
                - Recency: dias desde a última compra
                - Age: dias desde a primeira compra (antiguidade)
                - Frequency: número de compras
                - Monetary: valor médio das compras
                """
                # Criar cópia do dataframe
                df_seg = df.copy()
                
                # Definir condições de forma mutuamente exclusiva

                cond_novos = (df_seg['recencia'] <= 30) & (df_seg['antiguidade'] <= 30) # primeira compra no último mês

                cond_campeoes = (df_seg['recencia'] <= recencia_campeoes) & \
                                (df_seg['frequencia'] >= frequencia_campeoes) & (df_seg['vm_decil'] == vm_decil_campeoes) # clientes que compraram nos últimos 6 meses, possuem frequência de 37 ou mais e possuem valor monetário acima da média
                                
                cond_fieis_baixo_valor = (df_seg['recencia'] <= recencia_fieis) & (df_seg['antiguidade'] >= idade_fieis) & \
                            (df_seg['frequencia'] >= frequencia_fieis) & (df_seg['vm_decil'] <= vm_decil_fieis) # clientes há mais de 2 anos que compraram nos últimos 6 meses e possuem valor monetário menor ou igual a média
                
                cond_fieis_alto_valor = (df_seg['recencia'] <= recencia_fieis) & (df_seg['antiguidade'] >= idade_fieis) & \
                            (df_seg['frequencia'] >= frequencia_fieis) & (df_seg['vm_decil'] > vm_decil_fieis) # clientes há mais de 2 anos que compraram nos últimos 6 meses e possuem valor monetário acima da média
                            
                cond_recentes_alto = (df_seg['recencia'] <= recencia_recentes) & \
                                    (df_seg['frequencia'] >= frequencia_recentes) & (df_seg['vm_decil'] > vm_decil_recentes) # clientes que compraram nos últimos 6 meses e possui valor monetário acima da média
                                    
                cond_recentes_baixo = (df_seg['recencia'] <= recencia_recentes) & \
                                    (df_seg['frequencia'] >= frequencia_recentes) & (df_seg['vm_decil'] <= vm_decil_recentes) # clientes que compraram nos últimos 6 meses e possui valor monetário abaixo da média
                
                # Clientes menos ativos
                cond_sumidos = (df_seg['recencia'] > sumidos) & (df_seg['recencia'] <= inatividade) # última compra entre 6 meses e 2 anos
                cond_inativos = (df_seg['recencia'] > inatividade) # sem comprar faz 2 anos
                
                # Lista de condições e respectivos rótulos
                conditions = [
                    cond_novos,
                    cond_campeoes,
                    cond_fieis_baixo_valor,
                    cond_fieis_alto_valor,
                    cond_recentes_alto,
                    cond_recentes_baixo,
                    cond_sumidos,
                    cond_inativos
                ]
                
                labels = [
                    'Novos',
                    'Campeões',
                    'Fiéis Baixo Valor',
                    'Fiéis Alto Valor',
                    'Recentes Alto Valor',
                    'Recentes Baixo Valor',
                    'Sumidos',
                    'Inativos'
                ]
                
                # Aplicar segmentação
                df_seg['segmento'] = np.select(conditions, labels, default='Não Classificado')
                
                # Agregar dados para análise dos segmentos
                analise_segmentos = df_seg.groupby('segmento').agg({
                    'id_cliente': 'count',
                    'recencia': 'mean',
                    'frequencia': 'mean',
                    'valor_monetario': 'mean',
                    'antiguidade': 'mean'
                }).round(2)
                
                analise_segmentos.columns = [
                    'Quantidade Clientes',
                    'media_recencia_dias',
                    'media_frequencia',
                    'media_valor_monetario',
                    'media_antiguidade_dias'
                ]
                
                # Ordenar por quantidade de clientes
                analise_segmentos = analise_segmentos.sort_values('Quantidade Clientes', ascending=False)
                
                return df_seg, analise_segmentos
            
            rfma_segmentado, analise_segmentos = segment_customers(rfma)
            
            print(f"\nAnálise Detalhada por Segmento ({tipo_nome}):")
            print("=" * 120)
            print(analise_segmentos)
            
            # Mesclar dados para o arquivo final
            if tipo == 'PF':
                rfma_segmentado = rfma_segmentado.merge(df_clientes_PF[['id_cliente', 'cpf']], on='id_cliente', how='left')
                rfma_segmentado = rfma_segmentado.merge(df_clientes[['id_cliente', 'nome', 'email', 'telefone']], on='id_cliente', how='left')
            else:  # PJ
                rfma_segmentado = rfma_segmentado.merge(df_clientes_PJ[['id_cliente', 'cnpj']], on='id_cliente', how='left')
                rfma_segmentado = rfma_segmentado.merge(df_clientes[['id_cliente', 'nome', 'email', 'telefone']], on='id_cliente', how='left')
                
            rfma_segmentado['tipo_pessoa'] = tipo
            
            # Salvar arquivo individual
            # rfma_segmentado.to_csv(os.path.join(diretorio_cliente, f"analise_cliente_{tipo.lower()}.csv"), index=False)
            
            # Adicionar à lista de resultados para combinar depois
            resultados.append(rfma_segmentado)
            
        # Combinar ambos os dataframes se existirem
        if resultados:
            rfma_segmentado = pd.concat(resultados)
            # rfma_segmentado.to_csv(os.path.join(diretorio_cliente, f"analise_cliente.csv"), index=False)
        else:
            print("Nenhum resultado foi gerado para combinar.")
            
    else:
        # Processamento normal sem separação
        print(f"Cliente {nome_cliente} configurado para segmentação unificada de PF e PJ")
        
        ##########################################################
        # Calcular as métricas RFMA
        ##########################################################
        
        # Recency: Número de dias desde a última compra
        if data_referencia:
            data_referencia = pd.to_datetime(data_referencia)
            print(f"Usando data de referência específica: {data_referencia.strftime('%Y-%m-%d')}")
        else:
            data_referencia = df_vendas['data_venda'].max()
            print(f"Usando data de referência atual: {data_referencia.strftime('%Y-%m-%d')}")

        recency = df_vendas.groupby('id_cliente')['data_venda'].max().apply(lambda x: (data_referencia - x).days)
        
        # Frequency: Número de pedidos únicos por cliente
        frequency = df_vendas.groupby('id_cliente')['id_venda'].nunique()
        
        # Monetary: Valor total gasto
        monetary = df_vendas.groupby('id_cliente')['total_venda'].sum()
        
        # Age (Antiguidade): Dias desde a primeira compra
        age = df_vendas.groupby('id_cliente')['data_venda'].min().apply(lambda x: (data_referencia - x).days)
        
        # Combinar métricas em um único DataFrame
        rfma = pd.DataFrame({
            'recencia': recency,
            'frequencia': frequency,
            'valor_monetario': monetary,
            'antiguidade': age
        })
        
        # Resetar o índice para ter id_cliente como coluna
        rfma = rfma.reset_index()
        
        # Adicionar algumas verificações
        print("\nEstatísticas das métricas RFMA:")
        print("\nRecency (dias):")
        print(rfma['recencia'].describe())
        print("\nFrequency (número de pedidos):")
        print(rfma['frequencia'].describe())
        print("\nMonetary (valor total):")
        print(rfma['valor_monetario'].describe())
        print("\nAge (dias desde primeira compra):")
        print(rfma['antiguidade'].describe())
        
        # Verificações adicionais
        print("\nContagens de controle:")
        print(f"Número total de clientes: {len(rfma)}")
        
        # Verificação adicional para Age
        print("\nVerificação de coerência:")
        print("Clientes com Age menor que Recency:", len(rfma[rfma['antiguidade'] < rfma['recencia']]))
        
        ##############################################################
        # RFMA -Análise de clientes por Decis
        ###############################################################
        
        # Converter as colunas para float (caso estejam em decimal.Decimal)
        for col in ['recencia', 'frequencia', 'valor_monetario', 'antiguidade']:
            rfma[col] = rfma[col].astype(float)
        
        # Criar decis para cada métrica RFMA
        # Primeiro vamos obter os bins para cada métrica
        _, r_bins = pd.qcut(rfma['recencia'], q=10, duplicates='drop', retbins=True)
        _, f_bins = pd.qcut(rfma['frequencia'], q=10, duplicates='drop', retbins=True)
        _, m_bins = pd.qcut(rfma['valor_monetario'], q=10, duplicates='drop', retbins=True)
        _, a_bins = pd.qcut(rfma['antiguidade'], q=10, duplicates='drop', retbins=True)
        
        # Agora criar os labels com o número correto de categorias
        r_labels = list(range(len(r_bins)-1, 0, -1))  # Ordem inversa para Recency
        f_labels = list(range(1, len(f_bins)))  # Ordem normal para os demais
        m_labels = list(range(1, len(m_bins)))
        a_labels = list(range(1, len(a_bins)))
        
        # Aplicar os decis
        rfma['r_decil'] = pd.qcut(rfma['recencia'], q=10, labels=r_labels, duplicates='drop')
        rfma['f_decil'] = pd.qcut(rfma['frequencia'], q=10, labels=f_labels, duplicates='drop')
        rfma['vm_decil'] = pd.qcut(rfma['valor_monetario'], q=10, labels=m_labels, duplicates='drop')
        rfma['a_decil'] = pd.qcut(rfma['antiguidade'], q=10, labels=a_labels, duplicates='drop')
        
        # Criar ranges para visualização
        rfma['r_range'] = pd.cut(
            rfma['recencia'], 
            bins=r_bins, 
            labels=[f"{r_bins[i]:.1f}-{r_bins[i+1]:.1f}" for i in range(len(r_bins)-1)], 
            include_lowest=True
        )
        
        rfma['f_range'] = pd.cut(
            rfma['frequencia'], 
            bins=f_bins, 
            labels=[f"{f_bins[i]:.1f}-{f_bins[i+1]:.1f}" for i in range(len(f_bins)-1)], 
            include_lowest=True
        )
        
        rfma['vm_range'] = pd.cut(
            rfma['valor_monetario'], 
            bins=m_bins, 
            labels=[f"{m_bins[i]:.1f}-{m_bins[i+1]:.1f}" for i in range(len(m_bins)-1)], 
            include_lowest=True
        )
        
        rfma['a_range'] = pd.cut(
            rfma['antiguidade'], 
            bins=a_bins, 
            labels=[f"{a_bins[i]:.1f}-{a_bins[i+1]:.1f}" for i in range(len(a_bins)-1)], 
            include_lowest=True
        )
        
        def segment_customers(df):
            """
            Segmenta clientes com base em regras atualizadas, de acordo com a análise da distribuição por faixa.
            
            Parâmetros:
            - Recency: dias desde a última compra
            - Age: dias desde a primeira compra (antiguidade)
            - Frequency: número de compras
            - Monetary: valor médio das compras
            """
            # Criar cópia do dataframe
            df_seg = df.copy()
            
            # Definir condições de forma mutuamente exclusiva

            cond_novos = (df_seg['recencia'] <= 30) & (df_seg['antiguidade'] <= 30) # primeira compra no último mês

            cond_campeoes = (df_seg['recencia'] <= recencia_campeoes) & \
                            (df_seg['frequencia'] >= frequencia_campeoes) & (df_seg['vm_decil'] == vm_decil_campeoes) # clientes que compraram nos últimos 6 meses, possuem frequência de 37 ou mais e possuem valor monetário acima da média
                            
            cond_fieis_baixo_valor = (df_seg['recencia'] <= recencia_fieis) & (df_seg['antiguidade'] >= idade_fieis) & \
                        (df_seg['frequencia'] >= frequencia_fieis) & (df_seg['vm_decil'] <= vm_decil_fieis) # clientes há mais de 2 anos que compraram nos últimos 6 meses e possuem valor monetário menor ou igual a média
            
            cond_fieis_alto_valor = (df_seg['recencia'] <= recencia_fieis) & (df_seg['antiguidade'] >= idade_fieis) & \
                        (df_seg['frequencia'] >= frequencia_fieis) & (df_seg['vm_decil'] > vm_decil_fieis) # clientes há mais de 2 anos que compraram nos últimos 6 meses e possuem valor monetário acima da média
                        
            cond_recentes_alto = (df_seg['recencia'] <= recencia_recentes) & \
                                (df_seg['frequencia'] >= frequencia_recentes) & (df_seg['vm_decil'] > vm_decil_recentes) # clientes que compraram nos últimos 6 meses e possui valor monetário acima da média
                                
            cond_recentes_baixo = (df_seg['recencia'] <= recencia_recentes) & \
                                (df_seg['frequencia'] >= frequencia_recentes) & (df_seg['vm_decil'] <= vm_decil_recentes) # clientes que compraram nos últimos 6 meses e possui valor monetário abaixo da média
            
            # Clientes menos ativos
            cond_sumidos = (df_seg['recencia'] > sumidos) & (df_seg['recencia'] <= inatividade) # última compra entre 6 meses e 2 anos
            cond_inativos = (df_seg['recencia'] > inatividade) # sem comprar faz 2 anos
            
            # Lista de condições e respectivos rótulos
            conditions = [
                cond_novos,
                cond_campeoes,
                cond_fieis_baixo_valor,
                cond_fieis_alto_valor,
                cond_recentes_alto,
                cond_recentes_baixo,
                cond_sumidos,
                cond_inativos
            ]
            
            labels = [
                'Novos',
                'Campeões',
                'Fiéis Baixo Valor',
                'Fiéis Alto Valor',
                'Recentes Alto Valor',
                'Recentes Baixo Valor',
                'Sumidos',
                'Inativos'
            ]
            
            # Aplicar segmentação
            df_seg['segmento'] = np.select(conditions, labels, default='Não Classificado')
            
            # Agregar dados para análise dos segmentos
            analise_segmentos = df_seg.groupby('segmento').agg({
                'id_cliente': 'count',
                'recencia': 'mean',
                'frequencia': 'mean',
                'valor_monetario': 'mean',
                'antiguidade': 'mean'
            }).round(2)
            
            analise_segmentos.columns = [
                'Quantidade Clientes',
                'media_recencia_dias',
                'media_frequencia',
                'media_valor_monetario',
                'media_antiguidade_dias'
            ]
            
            # Ordenar por quantidade de clientes
            analise_segmentos = analise_segmentos.sort_values('Quantidade Clientes', ascending=False)
            
            return df_seg, analise_segmentos
    
        # Segmentação unificada
        rfma_segmentado, analise = segment_customers(rfma)
        print("\nAnálise Detalhada por Segmento:")
        print("=" * 120)
        print(analise)
        
        # Mescla convencional
        rfma_segmentado = rfma_segmentado.merge(df_clientes_PF[['id_cliente', 'cpf']], on='id_cliente', how='left')
        rfma_segmentado = rfma_segmentado.merge(df_clientes_PJ[['id_cliente', 'cnpj']], on='id_cliente', how='left')
        rfma_segmentado = rfma_segmentado.merge(df_clientes[['id_cliente', 'nome', 'email', 'telefone']], on='id_cliente', how='left')
        
        # Para identificar facilmente o tipo de pessoa
        rfma_segmentado['tipo_pessoa'] = np.where(rfma_segmentado['cpf'].notna(), 'PF', 
                                                    np.where(rfma_segmentado['cnpj'].notna(), 'PJ', 'Não identificado'))

    # Se temos segmentações anteriores, atualizar o histórico
    if 'df_segmentacao_atual' in locals() and len(df_segmentacao_atual) > 0:
        print(f"Atualizando histórico com {len(df_segmentacao_atual)} segmentações anteriores...")
        
        # Criar um dicionário para acessar segmentos anteriores
        segmentacao_atual_dict = {}
        for _, row in df_segmentacao_atual.iterrows():
            segmentacao_atual_dict[row['id_cliente']] = {
                'segmento': row['segmento'],
                'penultima': row['penultima_segmentacao']
            }

        # Para cada cliente no novo dataframe de segmentação
        for idx, row in rfma_segmentado.iterrows():
            id_cliente = row['id_cliente']
            # Verificar se o cliente tem uma segmentação anterior
            if id_cliente in segmentacao_atual_dict:
                # Mover penultima para antepenultima
                rfma_segmentado.at[idx, 'antepenultima_segmentacao'] = segmentacao_atual_dict[id_cliente]['penultima']
                # Mover o segmento atual para penultima_segmentacao
                rfma_segmentado.at[idx, 'penultima_segmentacao'] = segmentacao_atual_dict[id_cliente]['segmento']
        
        print("Histórico de segmentação atualizado com sucesso!")
    else:
        print("Não foram encontradas segmentações anteriores para atualizar o histórico.")
        rfma_segmentado['antepenultima_segmentacao'] = None
        rfma_segmentado['penultima_segmentacao'] = None

    ###############################
    # Salvar relatório
    ###############################
    # Salvar o arquivo
    # rfma_segmentado.to_csv(os.path.join(diretorio_cliente, f"analise_cliente.csv"), index=False)

    # Inserir dados no banco de dados
    resultado_insercao = inserir_segmentacao_no_banco(database, rfma_segmentado)
    if resultado_insercao:
        print(f"Dados inseridos com sucesso no banco de dados {database}!")
    else:
        print(f"Falha ao inserir dados no banco de dados {database}.")

    rfma_segmentado_assistente = rfma_segmentado.copy()
    
    #selecionar colunas que vão para o assistente
    colunas_assistente = [
        'id_cliente',
        'recencia', 
        'frequencia', 
        'valor_monetario', 
        'antiguidade',
        'r_decil', 
        'f_decil', 
        'vm_decil', 
        'a_decil',
        'segmento'
    ]
    rfma_segmentado_assistente = rfma_segmentado_assistente[colunas_assistente]

    #inserir para assistente
    resultado_insercao_assistente = inserir_segmentacao_para_assistente(database, rfma_segmentado_assistente)
    if resultado_insercao_assistente:
        print(f"Dados para assistente inseridos com sucesso no banco de dados {database}!")
    else:
        print(f"Falha ao inserir dados para assistente no banco de dados {database}.")

    # print(f"Análise de clientes gerada para {nome_cliente}")

    # Dataframe de métricas gerais para o cliente
    print("\nGerando métricas de segmentação por tipo de pessoa (PF/PJ)...")
        
    # Criar contagem de clientes por segmento e tipo de pessoa
    contagem_pf = rfma_segmentado[rfma_segmentado['tipo_pessoa'] == 'PF']['segmento'].value_counts()
    contagem_pj = rfma_segmentado[rfma_segmentado['tipo_pessoa'] == 'PJ']['segmento'].value_counts()
    contagem_total = rfma_segmentado['segmento'].value_counts()  # Total por segmento (PF + PJ)

    # Usar a data de referência ou data atual para os arquivos
    if data_referencia:
        data_execucao = pd.to_datetime(data_referencia).strftime("%Y-%m-%d")
    else:
        data_execucao = datetime.now().strftime("%Y-%m-%d")

    # Função para remover acentos de strings
    def remover_acentos(texto):
        texto = unicodedata.normalize('NFKD', texto)
        return ''.join([c for c in texto if not unicodedata.combining(c)])
        
    # Criar um DataFrame com uma única linha
    segmentos_unicos = sorted(rfma_segmentado['segmento'].unique())
    metricas = {'data': data_execucao}  # Iniciar com a data de execução
        
    # Adicionar contagens de PF, PJ e total por segmento com nomes padronizados
    for segmento in segmentos_unicos:
        # Substituir espaços por underscores e remover caracteres especiais
        nome_segmento = remover_acentos(segmento).replace(' ', '_').replace('-', '_').lower()
        
        # Adicionar contagem de PF para o segmento
        metricas[f'pf_{nome_segmento}'] = contagem_pf.get(segmento, 0)
        
        # Adicionar contagem de PJ para o segmento
        metricas[f'pj_{nome_segmento}'] = contagem_pj.get(segmento, 0)
        
        # Adicionar contagem total (PF + PJ) para o segmento
        metricas[f'total_{nome_segmento}'] = contagem_total.get(segmento, 0)
            
    # Criar DataFrame com uma única linha
    metricas_df = pd.DataFrame([metricas])
        
    # Adicionar totais gerais
    metricas_df['total_pf'] = sum(contagem_pf)
    metricas_df['total_pj'] = sum(contagem_pj)
    metricas_df['total_geral'] = sum(contagem_total)

    # Adicionar a descrição como última coluna
    metricas_df['descricao'] = descricao
        
    # Exibir o resultado
    print("\nMétricas de Segmentação - Quantidade de Clientes por Segmento e Tipo de Pessoa:")
    print("=" * 100)
    print(metricas_df)

    ###############################
    # Salvar relatório
    ###############################
    # Salvar as métricas em arquivo separado
    # metricas_df.to_csv(os.path.join(diretorio_cliente, f"metricas_segmentacao_resumo.csv"), index=False)
    # print(f"Métricas de segmentação salvas em: {os.path.join(diretorio_cliente, 'metricas_segmentacao_resumo.csv')}")
        
    # Inserir métricas no banco de dados
    resultado_insercao = inserir_metricas_no_banco(database, metricas_df)
    if resultado_insercao:
        print(f"Métricas de segmentação inseridas com sucesso no banco de dados {database}!")
    else:
        print(f"Falha ao inserir métricas de segmentação no banco de dados {database}.")
    
def inserir_segmentacao_no_banco(database, rfma_segmentado):
    """
    Insere os dados de segmentação no banco de dados PostgreSQL.
    
    Parâmetros:
    - database: Nome do banco de dados
    - rfma_segmentado: DataFrame pandas com os dados de segmentação a serem inseridos
    
    Retorno:
    - bool: True se a inserção foi bem-sucedida, False caso contrário
    """
    try:
        # Conectar ao PostgreSQL
        print("Conectando ao banco de dados PostgreSQL para inserir segmentação...")
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
        
        # Verificar se a tabela já existe no esquema maloka_analytics
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='segmentacao' AND table_schema='maloka_analytics')")
        tabela_existe = cursor.fetchone()[0]
        
        if tabela_existe:
            # Verificar se as novas colunas existem na tabela
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='segmentacao' AND table_schema='maloka_analytics'")
            colunas_existentes = [row[0] for row in cursor.fetchall()]
                
            # Adicionar colunas que não existem ainda
            for coluna in rfma_segmentado.columns:
                if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                    print(f"Adicionando nova coluna: {coluna}")
                        
                    # Determinar o tipo de dados da coluna
                    dtype = rfma_segmentado[coluna].dtype
                    if 'int' in str(dtype):
                        tipo = 'INTEGER'
                    elif 'float' in str(dtype):
                        tipo = 'NUMERIC'
                    elif 'datetime' in str(dtype):
                        tipo = 'TIMESTAMP'
                    else:
                        tipo = 'TEXT'
                            
                    # Executar o ALTER TABLE para adicionar a coluna
                    try:
                        cursor.execute(f'ALTER TABLE maloka_analytics.segmentacao ADD COLUMN "{coluna}" {tipo}')
                        conn.commit()
                        print(f"Coluna {coluna} adicionada com sucesso!")
                    except Exception as e:
                        print(f"Erro ao adicionar coluna {coluna}: {e}")
                        conn.rollback()

            # Truncar a tabela se ela já existir
            print("Tabela segmentacao já existe no esquema maloka_analytics. Limpando dados existentes...")
            cursor.execute("TRUNCATE TABLE maloka_analytics.segmentacao")
        else:
            # Criar a tabela se não existir
            print("Criando tabela segmentacao no esquema maloka_analytics...")
            # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
            colunas = []
            primary_key_added = False
            for coluna, dtype in rfma_segmentado.dtypes.items():
                if coluna == 'id_cliente':
                    tipo = 'INTEGER PRIMARY KEY'
                    primary_key_added = True
                elif 'float' in str(dtype):
                    tipo = 'NUMERIC'
                elif 'datetime' in str(dtype):
                    tipo = 'TIMESTAMP'
                else:
                    tipo = 'TEXT'
                colunas.append(f'"{coluna}" {tipo}')

            # Se por algum motivo id_cliente não estava nas colunas, adicione a definição de chave primária
            if not primary_key_added:
                colunas.append('"id_cliente" INTEGER PRIMARY KEY')
            
            try:
                # Garantir que id_cliente seja PRIMARY KEY
                colunas_ajustadas = []
                has_id_cliente_pk = False
                
                for coluna in colunas:
                    if "id_cliente" in coluna.lower() and "primary key" in coluna.lower():
                        has_id_cliente_pk = True
                        colunas_ajustadas.append(coluna)
                    elif "id_cliente" in coluna.lower() and "primary key" not in coluna.lower():
                        has_id_cliente_pk = True
                        colunas_ajustadas.append(coluna.replace("INTEGER", "INTEGER PRIMARY KEY").replace("TEXT", "TEXT PRIMARY KEY"))
                    else:
                        colunas_ajustadas.append(coluna)
                
                if not has_id_cliente_pk:
                    colunas_ajustadas.append('"id_cliente" INTEGER PRIMARY KEY')
                
                create_table_query = f"""
                CREATE UNLOGGED TABLE maloka_analytics.segmentacao (
                    {", ".join(colunas_ajustadas)}
                )
                """
                print("Executando query de criação da tabela:")
                print(create_table_query)
                cursor.execute(create_table_query)
                conn.commit()  # Confirmar a criação da tabela
                print("Tabela UNLOGGED criada para inserção rápida")
            except Exception as e:
                print(f"Erro ao criar tabela maloka_analytics.segmentacao: {e}")
                print("Tentando criar novamente com definição de chave primária explícita...")
                
                # Tentar criar a tabela novamente com uma definição de chave primária explícita
                try:
                    create_table_query = """
                    CREATE UNLOGGED TABLE maloka_analytics.segmentacao (
                        "id_cliente" INTEGER PRIMARY KEY,
                        "recencia" INTEGER,
                        "frequencia" INTEGER,
                        "valor_monetario" NUMERIC,
                        "antiguidade" INTEGER,
                        "r_decil" INTEGER,
                        "f_decil" INTEGER,
                        "vm_decil" INTEGER,
                        "a_decil" INTEGER,
                        "segmento" TEXT
                    )
                    """
                    cursor.execute(create_table_query)
                    conn.commit()  # Confirmar a criação da tabela
                    print("Tabela criada com definição manual de colunas")
                except Exception as e2:
                    print(f"Falha na segunda tentativa de criar tabela: {e2}")
            
            # Definir fillfactor para reduzir fragmentação
            cursor.execute("ALTER TABLE maloka_analytics.segmentacao SET (fillfactor = 90)")
            
            # Otimizar configurações temporárias
            cursor.execute("SET maintenance_work_mem = '256MB'")
            cursor.execute("SET synchronous_commit = off")
        
        # Otimização da inserção de dados
        print(f"Preparando para inserir {len(rfma_segmentado)} registros na tabela segmentacao...")
        
        # Usar a função para inserir os dados em paralelo
        inserir_dados_paralelo(rfma_segmentado, "segmentacao", database, conn, cursor, "maloka_analytics")

        print(f"Dados inseridos com sucesso em maloka_analytics! Total de {len(rfma_segmentado)} registros.")        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Erro ao inserir dados no banco: {e}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return False
    
def inserir_segmentacao_para_assistente(database, rfma_segmentado_assistente):
    """
    Insere os dados de segmentação no banco de dados PostgreSQL.
    
    Parâmetros:
    - database: Nome do banco de dados
    - rfma_segmentado: DataFrame pandas com os dados de segmentação a serem inseridos
    
    Retorno:
    - bool: True se a inserção foi bem-sucedida, False caso contrário
    """

    try:
        # Conectar ao PostgreSQL
        print("Conectando ao banco de dados PostgreSQL para inserir segmentação...")
        conn = psycopg2.connect(
            host=DB_CONFIG_MALOKA['host'],
            database=database,
            user=DB_CONFIG_MALOKA['user'],
            password=DB_CONFIG_MALOKA['password'],
            port=DB_CONFIG_MALOKA['port']
        )
        
        # Criar cursor
        cursor = conn.cursor()
        
        # Verificar se o esquema maloka_core existe, caso contrário, criar
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'maloka_core')")
        schema_existe = cursor.fetchone()[0]
        
        if not schema_existe:
            print(f"Esquema maloka_core não existe no banco {database}. Criando...")
            cursor.execute("CREATE SCHEMA maloka_core")
        else: 
            print(f"Esquema maloka_core já existe no banco {database}.")

        # Verificar se a tabela já existe no esquema maloka_core
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='segmentacao' AND table_schema='maloka_core')")
        tabela_existe = cursor.fetchone()[0]
        
        if tabela_existe:
            # Verificar se as novas colunas existem na tabela
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='segmentacao' AND table_schema='maloka_core'")
            colunas_existentes = [row[0] for row in cursor.fetchall()]
                
            # Adicionar colunas que não existem ainda
            for coluna in rfma_segmentado_assistente.columns:
                if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                    print(f"Adicionando nova coluna: {coluna}")
                        
                    # Determinar o tipo de dados da coluna
                    dtype = rfma_segmentado_assistente[coluna].dtype
                    if 'int' in str(dtype):
                        tipo = 'INTEGER'
                    elif 'float' in str(dtype):
                        tipo = 'NUMERIC'
                    elif 'datetime' in str(dtype):
                        tipo = 'TIMESTAMP'
                    else:
                        tipo = 'TEXT'
                            
                    # Executar o ALTER TABLE para adicionar a coluna
                    try:
                        cursor.execute(f'ALTER TABLE maloka_core.segmentacao ADD COLUMN "{coluna}" {tipo}')
                        conn.commit()
                        print(f"Coluna {coluna} adicionada com sucesso!")
                    except Exception as e:
                        print(f"Erro ao adicionar coluna {coluna}: {e}")
                        conn.rollback()

            # Truncar a tabela se ela já existir
            print("Tabela segmentacao já existe no esquema maloka_core. Limpando dados existentes...")
            cursor.execute("TRUNCATE TABLE maloka_core.segmentacao")
        else:
            # Criar a tabela se não existir
            print("Criando tabela segmentacao no esquema maloka_core...")
            # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
            colunas = []
            primary_key_added = False
            for coluna, dtype in rfma_segmentado_assistente.dtypes.items():
                if coluna == 'id_cliente':
                    tipo = 'INTEGER PRIMARY KEY'
                    primary_key_added = True
                elif 'int' in str(dtype):
                    tipo = 'INTEGER'
                elif 'float' in str(dtype):
                    tipo = 'NUMERIC'
                elif 'datetime' in str(dtype):
                    tipo = 'TIMESTAMP'
                else:
                    tipo = 'TEXT'
                colunas.append(f'"{coluna}" {tipo}')
            
            # Se por algum motivo id_cliente não estava nas colunas, adicione a definição de chave primária
            if not primary_key_added:
                colunas.append('"id_cliente" INTEGER PRIMARY KEY')
            
            try:
                # Garantir que id_cliente seja PRIMARY KEY
                colunas_ajustadas = []
                has_id_cliente_pk = False
                
                for coluna in colunas:
                    if "id_cliente" in coluna.lower() and "primary key" in coluna.lower():
                        has_id_cliente_pk = True
                        colunas_ajustadas.append(coluna)
                    elif "id_cliente" in coluna.lower() and "primary key" not in coluna.lower():
                        has_id_cliente_pk = True
                        colunas_ajustadas.append(coluna.replace("INTEGER", "INTEGER PRIMARY KEY").replace("TEXT", "TEXT PRIMARY KEY"))
                    else:
                        colunas_ajustadas.append(coluna)
                
                if not has_id_cliente_pk:
                    colunas_ajustadas.append('"id_cliente" INTEGER PRIMARY KEY')
                
                create_table_query = f"""
                CREATE UNLOGGED TABLE maloka_core.segmentacao (
                    {", ".join(colunas_ajustadas)}
                )
                """
                print("Executando query de criação da tabela:")
                print(create_table_query)
                cursor.execute(create_table_query)
                conn.commit()  # Confirmar a criação da tabela
                print("Tabela UNLOGGED criada para inserção rápida")
            except Exception as e:
                print(f"Erro ao criar tabela maloka_core.segmentacao: {e}")
                print("Tentando criar novamente com definição de chave primária explícita...")
                
                # Tentar criar a tabela novamente com uma definição de chave primária explícita
                try:
                    create_table_query = """
                    CREATE UNLOGGED TABLE maloka_core.segmentacao (
                        "id_cliente" INTEGER PRIMARY KEY,
                        "recencia" INTEGER,
                        "frequencia" INTEGER,
                        "valor_monetario" NUMERIC,
                        "antiguidade" INTEGER,
                        "r_decil" INTEGER,
                        "f_decil" INTEGER,
                        "vm_decil" INTEGER,
                        "a_decil" INTEGER,
                        "segmento" TEXT
                    )
                    """
                    cursor.execute(create_table_query)
                    conn.commit()  # Confirmar a criação da tabela
                    print("Tabela criada com definição manual de colunas")
                except Exception as e2:
                    print(f"Falha na segunda tentativa de criar tabela: {e2}")
            
            # Definir fillfactor para reduzir fragmentação
            cursor.execute("ALTER TABLE maloka_core.segmentacao SET (fillfactor = 90)")
            
            # Otimizar configurações temporárias
            cursor.execute("SET maintenance_work_mem = '256MB'")
            cursor.execute("SET synchronous_commit = off")
        
        # Otimização da inserção de dados
        print(f"Preparando para inserir {len(rfma_segmentado_assistente)} registros na tabela segmentacao...")
        
        # Verificar se a tabela maloka_core.segmentacao existe antes de tentar inserir dados
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='segmentacao' AND table_schema='maloka_core')")
        tabela_existe = cursor.fetchone()[0]
        
        if not tabela_existe:
            print("ERRO: A tabela maloka_core.segmentacao não existe após a tentativa de criação!")
            print("Tentando criar a tabela novamente...")
            
            # Último recurso - tentar criar a tabela novamente
            try:
                create_table_query = """
                CREATE UNLOGGED TABLE maloka_core.segmentacao (
                    "id_cliente" INTEGER PRIMARY KEY,
                    "recencia" INTEGER,
                    "frequencia" INTEGER,
                    "valor_monetario" NUMERIC,
                    "antiguidade" INTEGER,
                    "r_decil" INTEGER,
                    "f_decil" INTEGER,
                    "vm_decil" INTEGER,
                    "a_decil" INTEGER,
                    "segmento" TEXT
                )
                """
                cursor.execute(create_table_query)
                conn.commit()
                print("Tabela criada com sucesso no último recurso")
            except Exception as e:
                print(f"Falha na última tentativa de criar tabela: {e}")
                print("Não foi possível criar a tabela maloka_core.segmentacao")
                cursor.close()
                conn.close()
                return False
        
        # Usar a função para inserir os dados em paralelo
        resultado = inserir_dados_paralelo(rfma_segmentado_assistente, "segmentacao", database, conn, cursor, "maloka_core")
        
        if resultado:
            print(f"Dados inseridos com sucesso em maloka_core! Total de {len(rfma_segmentado_assistente)} registros.")
        else:
            print(f"Falha ao inserir dados em maloka_core. Verifique os logs para mais detalhes.")
            
        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Erro ao inserir dados no banco: {e}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return False
# Modificação na função inserir_metricas_no_banco
def inserir_metricas_no_banco(database, metricas_df):
    """
    Insere as métricas de segmentação no banco de dados PostgreSQL.
    
    Parâmetros:
    - database: Nome do banco de dados
    - metricas_df: DataFrame pandas com as métricas a serem inseridas
    
    Retorno:
    - bool: True se a inserção foi bem-sucedida, False caso contrário
    """
    conn = None
    try:
        # Conectar ao PostgreSQL
        print("Conectando ao banco de dados PostgreSQL para inserir métricas de segmentação...")
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
            conn.commit()
            print("Schema criado com sucesso.")
        
        # Verificar se a tabela já existe no esquema maloka_analytics
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='metricas_segmentacao' AND table_schema='maloka_analytics')")
        tabela_existe = cursor.fetchone()[0]
        
        if not tabela_existe:
            # Criar a tabela se não existir
            print("Criando tabela metricas_segmentacao no esquema maloka_analytics...")
            # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
            colunas = []
            for coluna, dtype in metricas_df.dtypes.items():
                if 'int' in str(dtype):
                    tipo = 'INTEGER'
                elif 'float' in str(dtype):
                    tipo = 'DECIMAL'
                elif coluna == 'data':
                    tipo = 'DATE'
                else:
                    tipo = 'TEXT'
                colunas.append(f'"{coluna}" {tipo}')
            
            create_table_query = f"""
            CREATE TABLE maloka_analytics.metricas_segmentacao (
                {", ".join(colunas)}
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            print("Tabela criada com sucesso.")
        
        # Verificar se já existem métricas para esta data
        data = metricas_df['data'].iloc[0]
        print(f"Data a ser inserida: {data}")
        
        cursor.execute("SELECT COUNT(*) FROM maloka_analytics.metricas_segmentacao WHERE data = %s", 
                      (data,))
        
        registros_existentes = cursor.fetchone()[0]
        
        if registros_existentes > 0:
            print(f"Removendo {registros_existentes} registros existentes para a data {data}...")
            cursor.execute("DELETE FROM maloka_analytics.metricas_segmentacao WHERE data = %s", 
                          (data,))
            conn.commit()
            print("Registros antigos removidos com sucesso.")
        
        # Inserção dos dados
        print(f"Inserindo {len(metricas_df)} métricas de segmentação na tabela...")
        
        # Converter NaN para None
        df_upload = metricas_df.replace({np.nan: None})
        
        # Preparar a query
        colunas = [f'"{col}"' for col in df_upload.columns]
        placeholders = ", ".join(["%s"] * len(df_upload.columns))
        insert_query = f"""
        INSERT INTO maloka_analytics.metricas_segmentacao ({", ".join(colunas)})
        VALUES ({placeholders})
        """
        
        # Criar uma lista de tuplas com os valores
        valores = [tuple(row) for _, row in df_upload.iterrows()]
        
        print(f"Query de inserção: {insert_query}")
        print(f"Valores a serem inseridos: {valores[0]}")
        
        # Executar a inserção
        cursor.executemany(insert_query, valores)
        conn.commit()

        criar_indice_ordenacao(cursor)
        print("View de ordenação criada com sucesso.")
        
        # Verificar se a inserção foi bem sucedida
        cursor.execute("SELECT COUNT(*) FROM maloka_analytics.metricas_segmentacao WHERE data = %s", 
                      (data,))
        novos_registros = cursor.fetchone()[0]
        
        print(f"Verificação pós-inserção: {novos_registros} registros encontrados para a data {data}")
        
        print(f"Métricas de segmentação inseridas com sucesso para o cliente {database}!")
        
        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Erro ao inserir métricas no banco: {str(e)}")
        if conn:
            conn.rollback()  # Desfazer qualquer alteração pendente
            conn.close()
        return False

def criar_indice_ordenacao(cursor):
    """Cria índice para ordenação eficiente por data"""
    try:
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metricas_segmentacao_data 
        ON maloka_analytics.metricas_segmentacao (data ASC)
        """)
        print("Índice de ordenação criado com sucesso.")
    except Exception as e:
        print(f"Erro ao criar índice: {e}")

if __name__ == "__main__":
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Gera relatórios de faturamento para um cliente específico')
    parser.add_argument('cliente', type=str, nargs='?', default='todos', help='Nome do cliente para gerar relatórios (opcional, padrão: todos)')
    parser.add_argument('--data', type=str, help='Data de referência (YYYY-MM-DD). Se não especificada, usa data atual')
    
    # Parse dos argumentos
    args = parser.parse_args()
    
    # Verificar se o usuário solicitou processamento de todos os clientes
    if args.cliente.lower() == 'todos':
        print("Gerando relatórios para todos os clientes...")
        for cliente in CLIENTES.keys():
            print("\n" + "="*50)
            print(f"Processando cliente: {cliente}")
            print("="*50)
            gerar_analise_cliente(cliente, args.data)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_analise_cliente(args.cliente, args.data)

"""
Para executar um cliente específico, use o comando:
python analise_segmentacao.py cliente_nome

Para executar para todos os clientes, use o comando:
python analise_segmentacao.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python analise_segmentacao.py

# Para data atual
python analise_segmentacao.py cliente_nome

# Para data específica
python analise_segmentacao.py cliente_nome --data 2024-01-31

# Todos os clientes para data específica
python analise_segmentacao.py todos --data 2024-01-31
"""