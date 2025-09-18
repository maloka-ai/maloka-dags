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
        
def gerar_relatorios_orcamento(nome_cliente):
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

    except Exception as e:
        print(f"Erro: {e}")
        print("\nVerifique se:")
        print("1. O PostgreSQL está rodando")
        print("2. O banco de dados existe")
        print("3. As credenciais de conexão estão corretas")
        print("4. O esquema e as tabelas existem para este cliente")
    
    ########################################################
    # Faturamento PEDIDO X ORCAMENTO por cliente
    ########################################################

    #Pegar clientes ativos nos últimos 12 meses
    data_atual = datetime.now()
    data_limite = data_atual - pd.DateOffset(months=12)
    clientes_ativos = df_vendas[(df_vendas['data_venda'] >= data_limite) & (df_vendas['data_venda'] <= data_atual)]['id_cliente'].unique()
    df_clientes_ativos = df_clientes[df_clientes['id_cliente'].isin(clientes_ativos)]

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


    

    # Número de clientes ativos
    num_clientes_ativos = len(df_clientes_orcamento_pedido)
    print(f"\nNúmero de clientes ativos nos últimos 6 meses: {num_clientes_ativos}")

    print("\nRelatório de Clientes Ativos - Orçamentos x Pedidos Concluídos (últimos 6 meses):")
    print(df_clientes_orcamento_pedido.head())

if __name__ == "__main__":
    # Exemplo de execução para o cliente "add"
    gerar_relatorios_orcamento("add")