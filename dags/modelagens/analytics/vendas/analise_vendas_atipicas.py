import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from datetime import datetime
import psycopg2
import os
import warnings
import argparse
from datetime import datetime, timedelta
import sys
import numpy as np
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
from dags.modelagens.analytics.config_clientes import CLIENTES
from config.airflow_variables import DB_CONFIG_MALOKA

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def gerar_analise_vendas_atipicas(nome_cliente):
    """
    Gera análise de vendas atípicas para um cliente específico.
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
    analise_varejo = config_cliente['analise_varejo']
    if analise_varejo:
        print("Variaveis para Varejo")
        tempo_metodo_empirico_em_dias = config_cliente['tempo_metodo_empirico']
        tempo_analise_estatistico = config_cliente['tempo_analise_estatistico']
        min_atipicidade = config_cliente['minimo_de_atipicidade']
        valor_ruptura_estoque = config_cliente['valor_ruptura_estoque']
        desvio_padrao = config_cliente['valor_desvio_padrao']
    else:
        print("Variaveis para Atacado")
        tempo_analise_atipicidade = config_cliente['tempo_analise_atipicidade']
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    
    # Criar diretório para salvar os relatórios do cliente
    # diretorio_vendas_atipicas = os.path.join(diretorio_atual, 'relatorio_vendas_atipicas', nome_cliente)
    # os.makedirs(diretorio_vendas_atipicas, exist_ok=True)
    
    print(f"Gerando relatórios para o cliente: {nome_cliente}")
    print(f"Database: {database}, Schema: {schema}")
    def conectar_banco():
        """Estabelece conexão com o banco de dados PostgreSQL."""
        try:
            print("Conectando ao banco de dados PostgreSQL...")
            conn = psycopg2.connect(
                host=DB_CONFIG_MALOKA['host'],
                database=database,
                user=DB_CONFIG_MALOKA['user'],
                password=DB_CONFIG_MALOKA['password'],
                port=DB_CONFIG_MALOKA['port']
            )
            print("Conexão estabelecida com sucesso!")
            return conn
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            print("\nVerifique se:")
            print("1. O PostgreSQL está rodando")
            print("2. O banco de dados existe")
            print("3. As credenciais de conexão estão corretas")
            return None

    def obter_dados_base(conn):
        """Obtém os dados necessários para análise de vendas atípicas."""
        # Vendas e itens de venda
        query_vendas = f"""
            SELECT v.id_venda, v.data_venda, v.id_cliente, v.status, v.id_loja
            FROM {schema}.venda v
        """
        df_vendas = pd.read_sql(query_vendas, conn)
        
        query_venda_itens = f"""
            SELECT id_venda, id_produto, quantidade 
            FROM {schema}.venda_item
        """
        df_venda_itens = pd.read_sql(query_venda_itens, conn)
        
        # Clientes
        query_clientes = f"""
            SELECT id_cliente, nome
            FROM {schema}.cliente
        """
        df_clientes = pd.read_sql(query_clientes, conn)
        
        # Produtos
        query_produtos = f"""
            SELECT id_produto, nome
            FROM {schema}.produto
        """
        df_produtos = pd.read_sql(query_produtos, conn)

        # Loja
        query_lojas = f"""
            SELECT id_loja, nome
            FROM {schema}.loja
        """
        df_lojas = pd.read_sql(query_lojas, conn)
        
        # Estoque atualizado
        query_estoque = f"""
            SELECT DISTINCT ON (id_produto) 
                id_produto, 
                estoque_depois as estoque, 
                data_movimento as data_estoque_atualizado
            FROM {schema}.estoque_movimento 
            ORDER BY id_produto, data_movimento DESC
        """
        df_estoque = pd.read_sql(query_estoque, conn)

        # Compra
        query_compras = f"""
            SELECT id_produto, cobertura_dias
            FROM maloka_analytics.analise_recomendacao_compra
        """
        df_compras = pd.read_sql(query_compras, conn)
        
        return df_vendas, df_venda_itens, df_clientes, df_produtos, df_estoque, df_compras, df_lojas

    def preparar_dados_vendas(df_vendas, df_venda_itens):
        """Prepara e filtra os dados de vendas."""
        # Converter tipos
        df_vendas['id_venda'] = df_vendas['id_venda'].astype('int64')  # Converter para longint
        df_venda_itens['id_venda'] = df_venda_itens['id_venda'].astype('int64')  # Converter para longint
        
        # Mesclar vendas e itens
        df_combinado = df_venda_itens.merge(df_vendas, on='id_venda', how='left')
        
        # Filtrar colunas relevantes
        filtered_df = df_combinado[['id_venda', 'quantidade', 'data_venda', 'id_produto', 'id_cliente', 'id_loja']]
        
        # Filtrar vendas do último ano
        today = pd.Timestamp.now()

        #filto em tempo análise (anos)
        one_year_ago = today - pd.DateOffset(years=1)

        filtered_df = filtered_df[(filtered_df['data_venda'] >= one_year_ago) & (filtered_df['data_venda'] < today)]
        
        # Converter data_venda para datetime
        filtered_df['data_venda'] = pd.to_datetime(filtered_df['data_venda'])
        
        # Ordenar por data
        filtered_df = filtered_df.sort_values(by='data_venda')
        
        return filtered_df

    def identificar_anomalias(df):
        """Identifica vendas anômalas com base em z-score."""
        # Calcular média e desvio padrão da coluna 'quantidade'
        media = df['quantidade'].mean()
        desvio_padrao = df['quantidade'].std()
        
        # Se o desvio padrão for zero ou NaN, não há variação para calcular anomalias
        if desvio_padrao == 0 or pd.isna(desvio_padrao):
            return pd.DataFrame(), 0, 0   # Retorna DataFrame vazio
        
        # Calcular Z-score
        df['z_score'] = (df['quantidade'] - media) / desvio_padrao

        # Definir o limiar para outliers (Z > 3)
        limiar = 3
        outliers = df[df['z_score'] > limiar]

        # Definir a data de 15 dias atrás
        hoje = pd.Timestamp.today()
        duas_semanas_atras = hoje - pd.DateOffset(days=15)
        
        # Filtrar outliers que ocorreram nas últimas duas semanas
        outliers_duas_semana = outliers[outliers['data_venda'] >= duas_semanas_atras]
        
        return outliers_duas_semana, media, desvio_padrao

    def identificar_produtos_anomalos(df):
        """Identifica produtos com vendas anômalas."""
        ids = df['id_produto'].unique()
        anomalias = []
        
        for id in ids:
            # Filtrar vendas do produto
            sales = df[df['id_produto'] == id][['data_venda', 'quantidade', 'id_cliente', 'id_venda', 'id_loja']]
            
            # Agrupar por data e cliente
            # Pegar o primeiro id_venda em caso de agrupamento
            sales = sales.groupby(['data_venda', 'id_cliente'], as_index=False).agg({
                'quantidade': 'sum',
                'id_venda': 'first',  # Pegar o primeiro id_venda do grupo
                'id_loja': 'first'  # Pegar o primeiro id_loja do grupo
            })

            # Aplicar a função e identificar anomalias
            out, media, desvio = identificar_anomalias(sales)
        
            if len(out) > 0:
                print(f"*** Anomalias encontradas para produto {id}: {len(out)} ***")
                anomalias.append((id, out, media, desvio))
                
        print(f"Total de produtos com anomalias: {len(anomalias)}")
        return anomalias

    def gerar_relatorio_vendas_atipicas(anomalias, df_produtos, df_clientes, df_estoque, df_compras, df_lojas):
        """Gera relatório de vendas atípicas."""
        vendas_atipicas = []
        
        for id, info_df, media, desvio in anomalias:
            # Obter informações do produto
            produto_info = df_produtos[df_produtos['id_produto'] == id]
            if produto_info.empty:
                print(f"Produto ID {id} não encontrado no cadastro")
                continue
                
            produto = produto_info['nome'].iloc[0]
            
            # Obter estoque atual
            estoque_info = df_estoque[df_estoque['id_produto'] == id]
            estoque = estoque_info['estoque'].iloc[0] if not estoque_info.empty else 0

            # Obter cobertura de dias do produto
            cobertura_info = df_compras[df_compras['id_produto'] == id]
            cobertura_dias = cobertura_info['cobertura_dias'].iloc[0] if not cobertura_info.empty else 0

            # Criar dicionário para o produto
            d1 = {
                'id_produto': id,
                'produto': produto,
                'estoque_atualizado': estoque,
                'cobertura_dias': cobertura_dias,
                'media_vendas': round(media, 2),
                'desvio_padrao': round(desvio, 2),
                'vendas_atipicas': []
            }

            # Adicionar detalhes das vendas atípicas
            for _, row in info_df.iterrows():
                id_cliente = row['id_cliente']
                id_loja = row['id_loja']
                
                # Obter informações do cliente
                cliente_info = df_clientes[df_clientes['id_cliente'] == id_cliente]
                cliente = cliente_info['nome'].iloc[0] if not cliente_info.empty else "Cliente não identificado"

                # Obter informações da loja
                loja_info = df_lojas[df_lojas['id_loja'] == id_loja]
                loja = loja_info['nome'].iloc[0] if not loja_info.empty else "Loja não identificada"
                
                emissao = row['data_venda']
                quantidade = row['quantidade']
                id_venda = row['id_venda']

                d1["vendas_atipicas"].append({
                    "data": emissao,
                    "quantidade_atipica": quantidade,
                    "id_cliente": id_cliente,
                    "cliente": str(cliente),
                    "id_venda": id_venda,
                    "id_loja": id_loja,
                    "loja": str(loja)
                })
        
            vendas_atipicas.append(d1)

        # Verificar se há anomalias antes de criar o DataFrame
        if len(vendas_atipicas) > 0:
            # Criar DataFrame normalizado dos resultados
            df_r = pd.json_normalize(vendas_atipicas, record_path=["vendas_atipicas"], meta=[
                "id_produto", "produto", "estoque_atualizado", "cobertura_dias", "media_vendas", "desvio_padrao"
            ])
            df_r.sort_values("data", inplace=True)

            # Renomear coluna 'data' para 'data_venda' conforme alteração no banco
            df_r = df_r.rename(columns={'data': 'data_venda'})

            # Converter para os tipos corretos antes de enviar ao banco
            df_r['data_venda'] = pd.to_datetime(df_r['data_venda'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Converter IDs para Int64 com tratamento de NaN
            # Use pandas Int64 (nullable integer) para lidar com valores NaN
            for col in ['id_venda', 'id_produto', 'id_loja', 'id_cliente', 'quantidade_atipica', 'estoque_atualizado']:
                df_r[col] = pd.to_numeric(df_r[col], errors='coerce')
                # Remover NaN antes de converter para Int64 ou substituir por um valor padrão
                # df_r[col] = df_r[col].fillna(-1)  # Substituir NaN por -1 se for uma opção
            
            # Para colunas decimais
            df_r['cobertura_dias'] = pd.to_numeric(df_r['cobertura_dias'], errors='coerce')
            df_r['media_vendas'] = pd.to_numeric(df_r['media_vendas'], errors='coerce')
            df_r['desvio_padrao'] = pd.to_numeric(df_r['desvio_padrao'], errors='coerce')
            
            # Adicionar colunas padronizadas
            if 'tipo_atipicidade' not in df_r.columns:
                df_r['tipo_atipicidade'] = 'acima'  # Para atacado sempre definimos como 'acima'
            else:
                # Garantir que para atacado sempre será 'acima', independentemente do valor original
                df_r['tipo_atipicidade'] = 'acima'
            
            #valor_desvio none
            if 'valor_desvio' not in df_r.columns:
                df_r['valor_desvio'] = None

        else:
            # Criar um DataFrame vazio com as colunas esperadas e nomes atualizados
            df_r = pd.DataFrame(columns=[
                "data_venda", 
                "quantidade_atipica",
                "id_cliente",
                "cliente", 
                "id_venda", 
                "id_loja",
                "loja",
                "id_produto", 
                "produto", 
                "estoque_atualizado", 
                "cobertura_dias", 
                "media_vendas", 
                "desvio_padrao",
                "valor_desvio",
                "tipo_atipicidade"
            ])
            # Garantir que mesmo em DataFrame vazio, o tipo_atipicidade está definido como 'acima'
            df_r['tipo_atipicidade'] = 'acima'
        
        return df_r
    
    def identificar_anomalias_metodo_empirico(df, tempo_metodo_empirico_em_dias):
        """Identifica vendas anômalas usando o método empírico para varejistas.
        Considera como atípica vendas com quantidade pelo menos 3x acima da média
        dos últimos X dias (definido por tempo_metodo_empirico_em_dias)."""
        
        # Preparar dataframe para análise
        df = df.copy()
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        df = df.sort_values('data_venda')
        
        # Agrupar por produto
        ids_produtos = df['id_produto'].unique()
        anomalias = []
        
        hoje = pd.Timestamp.today()
        periodo_analise = hoje - pd.DateOffset(days=15)  # Analisar apenas últimas 2 semanas
        
        for id_produto in ids_produtos:
            # Filtrar vendas do produto
            vendas_produto = df[df['id_produto'] == id_produto].copy()
            
            # Analisar cada venda das últimas 2 semanas
            vendas_recentes = vendas_produto[vendas_produto['data_venda'] >= periodo_analise]
            
            for idx, venda in vendas_recentes.iterrows():
                data_venda = venda['data_venda']
                
                # Definir período de referência para calcular a média
                data_inicio_referencia = data_venda - pd.DateOffset(days=tempo_metodo_empirico_em_dias)
                
                # Pegar vendas do período de referência (excluindo a venda atual)
                vendas_referencia = vendas_produto[
                    (vendas_produto['data_venda'] >= data_inicio_referencia) & 
                    (vendas_produto['data_venda'] < data_venda)
                ]
                
                
                # Se ainda não houver vendas, não podemos determinar se é atípica
                if len(vendas_referencia) == 0:
                    continue
                    
                # Calcular média das quantidades no período de referência
                media_referencia = vendas_referencia['quantidade'].mean()
                
                # Verificar se a quantidade é pelo menos 3x maior que a média
                # E se atende aos novos critérios: quantidade > 4 e cobertura_dias <= 0
                if venda['quantidade'] >= (3 * media_referencia) and venda['quantidade'] >= min_atipicidade:
                    # Verificar cobertura_dias apenas se estiver presente no DataFrame
                    if 'cobertura_dias' in venda and venda['cobertura_dias'] <= valor_ruptura_estoque:
                        # Marcar como anômala e adicionar informações
                        venda_anomala = venda.copy()
                        venda_anomala['media_referencia'] = media_referencia
                        venda_anomala['razao'] = venda['quantidade'] / media_referencia
                        anomalias.append(venda_anomala)
                    elif 'cobertura_dias' not in venda:
                        # Se cobertura_dias não existir, consideramos a condição como atendida
                        venda_anomala = venda.copy()
                        venda_anomala['media_referencia'] = media_referencia
                        venda_anomala['razao'] = venda['quantidade'] / media_referencia
                        anomalias.append(venda_anomala)
        
        # Criar DataFrame com as anomalias
        if anomalias:
            df_anomalias = pd.DataFrame(anomalias)
            return df_anomalias, True
        else:
            return pd.DataFrame(), False
        
    def identificar_anomalias_metodo_estatistico(df):
        """
        Identifica vendas anômalas usando o método estatístico de Tukey (IQR).
        
        Para cada produto:
        1. Considera todas as quantidades vendidas no último ano
        2. Calcula Q1, Q3 e IQR
        3. Define limites: Q1 - 1.5*IQR e Q3 + 1.5*IQR
        4. Identifica vendas fora desses limites como atípicas
        
        Retorna apenas anomalias das últimas duas semanas.
        """
        
        # Preparar dataframe para análise
        df = df.copy()
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        df = df.sort_values('data_venda')
        
        # Definir período para análise (últimas duas semanas)
        hoje = pd.Timestamp.today()
        duas_semanas_atras = hoje - pd.DateOffset(days=15)
        
        # Lista para armazenar anomalias encontradas
        anomalias = []
        
        # Para cada produto
        for id_produto in df['id_produto'].unique():
            # Filtrar vendas do produto no último ano
            vendas_produto = df[df['id_produto'] == id_produto].copy()
            
            # Se houver poucas vendas (menos de 5), não é possível aplicar o método estatístico de forma confiável
            if len(vendas_produto) < 5:
                continue
            
            # Calcular quartis e IQR
            Q1 = vendas_produto['quantidade'].quantile(0.25)
            Q3 = vendas_produto['quantidade'].quantile(0.75)
            IQR = Q3 - Q1
            
            # Definir limites
            limite_inferior = Q1 - 1.5 * IQR
            limite_superior = Q3 + 1.5 * IQR
            
            # Identificar anomalias (apenas nas últimas duas semanas)
            vendas_recentes = vendas_produto[vendas_produto['data_venda'] >= duas_semanas_atras]
            
            for _, venda in vendas_recentes.iterrows():
                quantidade = venda['quantidade']
                
                # Verificar se está fora dos limites e se atende aos novos critérios
                if (quantidade > limite_superior or quantidade < limite_inferior) and quantidade >= min_atipicidade:
                    # Verificar a cobertura de dias, se disponível
                    cobertura_dias_ok = False
                    
                    if 'cobertura_dias' in venda:
                        if venda['cobertura_dias'] <= valor_ruptura_estoque:
                            cobertura_dias_ok = True
                    else:
                        # Se não tiver informação de cobertura, assume que está ok
                        cobertura_dias_ok = True
                    
                    if cobertura_dias_ok:
                        # Adicionar informações extras para análise
                        venda_anomala = venda.copy()
                        venda_anomala['Q1'] = Q1
                        venda_anomala['Q3'] = Q3
                        venda_anomala['IQR'] = IQR
                        venda_anomala['limite_inferior'] = limite_inferior
                        venda_anomala['limite_superior'] = limite_superior
                        
                        # Calcular o desvio (quanto a venda está fora do limite)
                        if quantidade > limite_superior:
                            venda_anomala['tipo_anomalia'] = 'acima'
                            venda_anomala['desvio'] = quantidade - limite_superior
                        else:
                            venda_anomala['tipo_anomalia'] = 'abaixo'
                            venda_anomala['desvio'] = limite_inferior - quantidade
                        
                        anomalias.append(venda_anomala)
        
        # Criar DataFrame com as anomalias
        if anomalias:
            df_anomalias = pd.DataFrame(anomalias)
            return df_anomalias, True
        else:
            return pd.DataFrame(), False
        
    # def identificar_anomalias_metodo_modified_zscore(df, tempo_analise_estatistico):
    #     """
    #     Identifica vendas anômalas usando o método estatístico Modified Z-Score.
        
    #     Para cada produto:
    #     1. Considera as quantidades vendidas no período definido por tempo_analise_estatistico
    #     2. Calcula a mediana e o MAD (desvio absoluto da mediana) das quantidades
    #     3. Para cada venda recente (últimos 15 dias), calcula M = 0.6745 * ((qtd - mediana) / MAD)
    #     4. Marca como atípica se |M| > 3.5
        
    #     Retorna apenas anomalias das últimas duas semanas.
    #     """
        
    #     # Preparar dataframe para análise
    #     df = df.copy()
    #     df['data_venda'] = pd.to_datetime(df['data_venda'])
    #     df = df.sort_values('data_venda')
        
    #     # Definir período para análise estatística baseado no tempo_analise_estatistico
    #     hoje = pd.Timestamp.today()
    #     periodo_estatistico = hoje - pd.DateOffset(days=tempo_analise_estatistico)
        
    #     # Definir período fixo de 15 dias para retornar apenas anomalias recentes
    #     duas_semanas_atras = hoje - pd.DateOffset(days=15)
        
    #     # Lista para armazenar anomalias encontradas
    #     anomalias = []
        
    #     # Para cada produto
    #     for id_produto in df['id_produto'].unique():
    #         # Filtrar vendas do produto no período de análise estatística
    #         vendas_periodo_analise = df[
    #             (df['id_produto'] == id_produto) & 
    #             (df['data_venda'] >= periodo_estatistico)
    #         ].copy()
            
    #         # Se houver poucas vendas (menos de 5), não é possível aplicar o método estatístico de forma confiável
    #         if len(vendas_periodo_analise) < 5:
    #             continue
            
    #         # Calcular mediana das quantidades
    #         mediana = vendas_periodo_analise['quantidade'].median()
            
    #         # Calcular MAD (Median Absolute Deviation)
    #         # MAD = mediana(|Xi - mediana|)
    #         mad = np.median(np.abs(vendas_periodo_analise['quantidade'] - mediana))
            
    #         # Se MAD for zero, não é possível calcular o Modified Z-Score
    #         if mad == 0:
    #             continue
            
    #         # Identificar anomalias apenas nas últimas duas semanas
    #         vendas_recentes = df[
    #             (df['id_produto'] == id_produto) & 
    #             (df['data_venda'] >= duas_semanas_atras)
    #         ]

    #         for _, venda in vendas_recentes.iterrows():
    #             quantidade = venda['quantidade']
                
    #             # Calcular Modified Z-Score
    #             # M = 0.6745 * ((X - mediana) / MAD)
    #             modified_zscore = 0.6745 * ((quantidade - mediana) / mad)
                
    #             # Verificar se está fora dos limites (|M| > 3.5)
    #             # Acima de 2.0 não encontro atipicidade para a bibi
    #             # Permanece com 2 atípicos até o valor cair para 0.
    #             if abs(modified_zscore) > desvio_padrao:
    #                 # Adicionar informações extras para análise
    #                 venda_anomala = venda.copy()
    #                 venda_anomala['mediana'] = mediana
    #                 venda_anomala['mad'] = mad
    #                 venda_anomala['modified_zscore'] = modified_zscore
                    
    #                 # Marcar tipo de anomalia
    #                 if modified_zscore > 0:
    #                     venda_anomala['tipo_anomalia'] = 'acima'
    #                 else:
    #                     venda_anomala['tipo_anomalia'] = 'abaixo'
                    
    #                 anomalias.append(venda_anomala)
        
    #     # Criar DataFrame com as anomalias
    #     if anomalias:
    #         df_anomalias = pd.DataFrame(anomalias)
    #         return df_anomalias, True
    #     else:
    #         return pd.DataFrame(), False

    def padronizar_dataframe_vendas_atipicas(df, analise_varejo=True):
        """
        Padroniza o DataFrame de vendas atípicas para garantir a mesma estrutura 
        independentemente de ser varejista ou atacadista.
        
        Args:
            df: DataFrame de vendas atípicas
            analise_varejo: Indica se é análise de varejo (True) ou atacado (False)
        
        Returns:
            DataFrame padronizado
        """
        if df.empty:
            # Definir as colunas padronizadas para um DataFrame vazio
            colunas_padrao = [
                'data_venda', 'quantidade_atipica', 'id_cliente', 'cliente', 
                'id_venda', 'id_loja', 'loja', 'id_produto', 'produto',
                'estoque_atualizado', 'cobertura_dias', 'media_vendas',
                'valor_desvio', 'tipo_atipicidade'
            ]
            return pd.DataFrame(columns=colunas_padrao)
        
        df_padronizado = df.copy()
        
        # Renomear colunas conforme origem (varejo ou atacado)
        if analise_varejo:
            # Para varejo, verificar e ajustar colunas específicas
            if 'desvio' in df_padronizado.columns and 'valor_desvio' not in df_padronizado.columns:
                df_padronizado = df_padronizado.rename(columns={'desvio': 'valor_desvio'})
            
            if 'tipo_anomalia' in df_padronizado.columns and 'tipo_atipicidade' not in df_padronizado.columns:
                df_padronizado = df_padronizado.rename(columns={'tipo_anomalia': 'tipo_atipicidade'})
                
            # Garantir que tenha a coluna media_vendas
            if 'media_vendas' not in df_padronizado.columns:
                df_padronizado['media_vendas'] = None
            
        else:
            # Para atacado, consolidar desvio_padrao em valor_desvio
            if 'desvio_padrao' in df_padronizado.columns:
                # Se valor_desvio não existir, usar o valor de desvio_padrao
                if 'valor_desvio' not in df_padronizado.columns:
                    df_padronizado['valor_desvio'] = df_padronizado['desvio_padrao']
                # Se valor_desvio for None/NaN, usar o valor de desvio_padrao
                else:
                    df_padronizado['valor_desvio'] = df_padronizado['valor_desvio'].fillna(df_padronizado['desvio_padrao'])
                
                # Remover a coluna desvio_padrao
                df_padronizado = df_padronizado.drop(columns=['desvio_padrao'], errors='ignore')
            
            # Se não tiver valor_desvio após a consolidação
            if 'valor_desvio' not in df_padronizado.columns:
                df_padronizado['valor_desvio'] = None
            
            if 'tipo_atipicidade' not in df_padronizado.columns:
                # Definir valor padrão baseado na média
                df_padronizado['tipo_atipicidade'] = 'acima'  # Atacado geralmente analisa valores acima da média
        
        # Garantir todas as colunas necessárias estejam presentes
        colunas_padrao = [
            'data_venda', 'quantidade_atipica', 'id_cliente', 'cliente', 
            'id_venda', 'id_loja', 'loja', 'id_produto', 'produto',
            'estoque_atualizado', 'cobertura_dias', 'media_vendas',
            'valor_desvio', 'tipo_atipicidade'
        ]
        
        for coluna in colunas_padrao:
            if coluna not in df_padronizado.columns:
                print(f"Adicionando coluna ausente: {coluna}")
                df_padronizado[coluna] = None
        
        # Manter apenas as colunas padrão
        df_padronizado = df_padronizado[colunas_padrao]
        
        return df_padronizado

    def garantir_tipagem_adequada(df):
        """
        Garante que as colunas do DataFrame estejam com os tipos adequados antes de exportar para o banco.
        Também renomeia a coluna 'data' para 'data_venda' se necessário.
        """
        if df.empty:
            return df
            
        df_tipado = df.copy()
        
        # Verificar se existe a coluna 'data' e renomeá-la para 'data_venda'
        if 'data' in df_tipado.columns and 'data_venda' not in df_tipado.columns:
            print("Renomeando coluna 'data' para 'data_venda'...")
            df_tipado = df_tipado.rename(columns={'data': 'data_venda'})
        
        # Aplicar tipos adequados para cada coluna
        for coluna in df_tipado.columns:
            # Definir o tipo com base no nome da coluna
            if coluna == 'id_produto':
                # id_produto pode ter valores muito grandes, usar int64 explicitamente
                df_tipado[coluna] = pd.to_numeric(df_tipado[coluna], errors='coerce')
                # Manter como int64 ou object se houver NaN
                if not df_tipado[coluna].isna().any():
                    try:
                        df_tipado[coluna] = df_tipado[coluna].astype('int64')
                    except Exception as e:
                        print(f"Aviso: Não foi possível converter id_produto para int64: {e}")

            elif coluna == 'id_cliente':
                # Tratar id_cliente separadamente para lidar com valores vazios
                df_tipado[coluna] = pd.to_numeric(df_tipado[coluna], errors='coerce')
                # Substituir valores NaN por -1 (ou outro valor que represente cliente não identificado)
                df_tipado[coluna] = df_tipado[coluna].fillna(-1)
                try:
                    # Converter para int64 depois de tratar os valores NaN
                    df_tipado[coluna] = df_tipado[coluna].astype('int64')
                except Exception as e:
                    print(f"Aviso: Não foi possível converter id_cliente para int64: {e}")
                    # Se ainda houver erro, mostrar valores problemáticos
                    print(f"Valores na coluna id_cliente: {df_tipado[coluna].unique()}")
            
            elif coluna == 'id_venda':
                # Converter para int64 (longint) se houver NaN
                df_tipado[coluna] = pd.to_numeric(df_tipado[coluna], errors='coerce').astype('Int64')
            elif coluna in ['id_loja', 'estoque_atualizado', 
                        'cobertura_dias', 'quantidade_atipica']:
                # Converter para numérico, mas não forçar a conversão para Int64 se houver NaN
                df_tipado[coluna] = pd.to_numeric(df_tipado[coluna], errors='coerce')
                # Verificar se há valores NaN antes de converter para Int64
                if not df_tipado[coluna].isna().any():
                    try:
                        df_tipado[coluna] = df_tipado[coluna].astype('Int64')
                    except Exception as e:
                        print(f"Aviso: Não foi possível converter coluna {coluna} para Int64: {e}")
            elif coluna in ['media_vendas', 'valor_desvio']:
                df_tipado[coluna] = pd.to_numeric(df_tipado[coluna], errors='coerce')
            elif coluna == 'data_venda':
                df_tipado[coluna] = pd.to_datetime(df_tipado[coluna], errors='coerce').dt.strftime('%Y-%m-%d')
        
        print("Tipagem das colunas verificada e ajustada para exportação ao banco.")
        return df_tipado

    def exportar_resultados(df, analise_varejo=None):
        """
        Exporta os resultados para o banco de dados.
        
        Args:
            df: DataFrame com os resultados
            analise_varejo: Se True, padroniza para varejo; se False, para atacado;
                           se None, usa a configuração global
        """
        try:
            if df.empty:
                print("Nenhuma venda atípica foi encontrada para exportar.")
                return
            
            # Usar o valor global se não for especificado
            if analise_varejo is None:
                analise_varejo = config_cliente['analise_varejo']
            
            # Padronizar o DataFrame antes de exportar
            df_padronizado = padronizar_dataframe_vendas_atipicas(df, analise_varejo)
            
            # Garantir a tipagem adequada antes de exportar
            df_padronizado = garantir_tipagem_adequada(df_padronizado)
            
            #colocar no banco
            try:
                # Reconectar ao PostgreSQL
                print("Reconectando ao banco de dados PostgreSQL...")
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
                cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='venda_atipica' AND table_schema='maloka_analytics')")
                tabela_existe = cursor.fetchone()[0]
                
                if tabela_existe:
                    # Verificar se as novas colunas existem na tabela
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='venda_atipica' AND table_schema='maloka_analytics'")
                    colunas_existentes = [row[0] for row in cursor.fetchall()]
                    
                    # Adicionar colunas que não existem ainda
                    for coluna in df_padronizado.columns:
                        if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                            print(f"Adicionando nova coluna: {coluna}")
                            
                            # Determinar o tipo de dados da coluna
                            dtype = df_padronizado[coluna].dtype
                            if coluna == 'id_produto':
                                tipo = 'INTEGER'
                            elif coluna == 'id_cliente':
                                tipo = 'INTEGER NULL'  # Permitir valores NULL para id_cliente
                            elif 'int' in str(dtype):
                                tipo = 'INTEGER'
                            elif 'float' in str(dtype):
                                tipo = 'DECIMAL'
                            elif 'datetime' in str(dtype):
                                tipo = 'TIMESTAMP'
                            else:
                                tipo = 'TEXT'
                                
                            # Executar o ALTER TABLE para adicionar a coluna
                            try:
                                cursor.execute(f'ALTER TABLE maloka_analytics.venda_atipica ADD COLUMN "{coluna}" {tipo}')
                                conn.commit()
                                print(f"Coluna {coluna} adicionada com sucesso!")
                            except Exception as e:
                                print(f"Erro ao adicionar coluna {coluna}: {e}")
                                conn.rollback()
                    
                    # Limpar os dados existentes
                    print("Limpando dados existentes...")
                    cursor.execute("TRUNCATE TABLE maloka_analytics.venda_atipica")
                    conn.commit()
                else:
                    # Criar a tabela se não existir
                    print("Criando tabela venda_atipica no esquema maloka_analytics...")
                    # Definir os tipos de dados para cada coluna com base nos tipos do DataFrame
                    colunas = []
                    for coluna in df_padronizado.columns:
                        # Definir o tipo com base no nome da coluna e nas alterações feitas
                        if coluna == 'id_produto':
                            tipo = 'INTEGER'
                        elif coluna == 'id_cliente':
                            tipo = 'INTEGER NULL'  # Permitir valores NULL para id_cliente
                        elif coluna == 'id_venda':
                            tipo = 'BIGINT'  # Usar BIGINT para id_venda
                        elif coluna in ['id_loja', 'estoque_atualizado', 
                            'cobertura_dias', 'quantidade_atipica']:
                            tipo = 'INTEGER'
                        elif coluna in ['media_vendas', 'valor_desvio']:
                            tipo = 'NUMERIC'
                        elif coluna == 'data_venda':
                            tipo = 'DATE'
                        else:
                            tipo = 'TEXT'
                        colunas.append(f'"{coluna}" {tipo}')
                    
                    create_table_query = f"""
                    CREATE TABLE maloka_analytics.venda_atipica (
                        {", ".join(colunas)}
                    )
                    """
                    cursor.execute(create_table_query)
                
                # Otimização da inserção de dados
                print(f"Inserindo {len(df_padronizado)} registros na tabela venda_atipica...")
                
                # Converter NaN para None
                df_upload = df_padronizado.replace({np.nan: None})

                # Converter DataFrame para tipos Python nativos que o psycopg2 aceita
                for col in df_upload.columns:
                    # Tratar vazios e NaN antes da conversão para tipos nativos
                    df_upload[col] = df_upload[col].replace({np.nan: None})
                    df_upload[col] = df_upload[col].replace({'': None})
                    
                    # Converter números para tipos nativos do Python
                    if pd.api.types.is_numeric_dtype(df_upload[col]):
                        df_upload[col] = df_upload[col].astype(object)
                    
                # Converter explicitamente valores numpy para Python nativos
                # Usar uma função segura para conversão que lida com None/NaN
                def convert_to_native(x):
                    if x is None or (hasattr(x, 'isna') and x.isna()):
                        return None
                    elif hasattr(x, 'item'):
                        try:
                            return x.item()
                        except:
                            return x
                    else:
                        return x
                
                # Aplicar a conversão segura para tipos nativos
                df_upload = df_upload.applymap(convert_to_native)
                
                # Aumentar o tamanho do lote para melhor performance
                batch_size = 5000
                
                # Preparar a query uma única vez fora do loop
                colunas = [f'"{col}"' for col in df_upload.columns]
                placeholders = ", ".join(["%s"] * len(df_upload.columns))
                insert_query = f"""
                INSERT INTO maloka_analytics.venda_atipica ({", ".join(colunas)})
                VALUES ({placeholders})
                """
                
                # Criar uma lista de tuplas com os valores
                valores = [tuple(row) for _, row in df_upload.iterrows()]
                
                # Executar a inserção em lotes
                cursor = conn.cursor()
                for i in range(0, len(valores), batch_size):
                    batch = valores[i:i+batch_size]
                    cursor.executemany(insert_query, batch)
                    # Commit a cada lote para não sobrecarregar a memória
                    conn.commit()
                
                print(f"Dados inseridos com sucesso! Total de {len(df_upload)} registros.")
                
                # Fechar cursor e conexão
                cursor.close()
                conn.close()
               
            except Exception as e:
                print(f"Erro ao inserir dados no banco: {e}")
                print("Detalhes do erro:")
                
                # Analisar o DataFrame para mostrar possíveis problemas de tipagem
                print("\nVerificando dados problemáticos:")
                for col in df_upload.columns:
                    if col in ['id_venda', 'id_produto', 'id_loja', 'id_cliente', 'estoque_atualizado', 
                             'cobertura_dias', 'quantidade_atipica']:
                        # Mostrar valores únicos para ajudar a identificar problemas
                        valores_unicos = df_upload[col].unique()
                        print(f"Coluna {col}: Tipo = {df_upload[col].dtype}, Valores únicos = {valores_unicos[:5]} ...")
                        
                        # Verificar valores que podem causar problemas
                        if pd.api.types.is_numeric_dtype(df_upload[col]):
                            grandes = df_upload[df_upload[col] > 2147483647]
                            if not grandes.empty:
                                print(f"  ⚠️ A coluna {col} tem {len(grandes)} valores maiores que o limite de INTEGER (2147483647)")
                
                if 'conn' in locals() and conn is not None:
                    conn.close()
                print(f"Análise de vendas atípicas gerada para {nome_cliente}, mas houve erros na exportação para o banco.")

        except Exception as e:
            print(f"Erro ao exportar arquivo: {e}")

    # Estabelecer conexão com banco de dados
    conn = conectar_banco()
    if conn is None:
        return
    
    # def visualizar_produtos_atipicos(nome_cliente, df_vendas_preparado, df_empirico=None, df_tukey=None, df_zscore=None, df_consolidado=None):
    #     """
    #     Gera visualizações de produtos com vendas atípicas, mostrando a série temporal completa
    #     de vendas no último ano e destacando os pontos atípicos.
        
    #     Args:
    #         nome_cliente: Nome do cliente para identificar diretório de saída
    #         df_vendas_preparado: DataFrame com histórico completo de vendas do último ano
    #         df_empirico: DataFrame com resultados do método empírico
    #         df_tukey: DataFrame com resultados do método Tukey
    #         df_zscore: DataFrame com resultados do método Z-Score
    #         df_consolidado: DataFrame consolidado (se já existir)
        
    #     Returns:
    #         Caminho do diretório onde as visualizações foram salvas
    #     """
    #     # Verificar se o cliente existe na configuração
    #     if nome_cliente not in CLIENTES:
    #         print(f"Erro: Cliente '{nome_cliente}' não encontrado na configuração!")
    #         print(f"Clientes disponíveis: {', '.join(CLIENTES.keys())}")
    #         return
            
    #     # Caminho para salvar as visualizações
    #     diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    #     diretorio_vendas_atipicas = os.path.join(diretorio_atual, 'relatorio_vendas_atipicas', nome_cliente)
        
    #     # Verificar se o diretório de resultados existe, senão criar
    #     os.makedirs(diretorio_vendas_atipicas, exist_ok=True)
        
    #     # Criar diretório para as visualizações
    #     diretorio_visualizacoes = os.path.join(diretorio_vendas_atipicas, 'visualizacoes')
    #     os.makedirs(diretorio_visualizacoes, exist_ok=True)

    #     # Criar subpastas para cada estratégia
    #     diretorio_empirico = os.path.join(diretorio_visualizacoes, 'metodo_empirico')
    #     diretorio_tukey = os.path.join(diretorio_visualizacoes, 'metodo_tukey')
    #     # diretorio_zscore = os.path.join(diretorio_visualizacoes, 'metodo_zscore')
    #     diretorio_combinado = os.path.join(diretorio_visualizacoes, 'metodos_combinados')
        
    #     # Criar diretórios para cada método
    #     os.makedirs(diretorio_empirico, exist_ok=True)
    #     os.makedirs(diretorio_tukey, exist_ok=True)
    #     # os.makedirs(diretorio_zscore, exist_ok=True)
    #     os.makedirs(diretorio_combinado, exist_ok=True)
        
    #     # Verificar se já temos um DataFrame consolidado ou precisamos criar um
    #     if df_consolidado is None:
    #         # Se não temos o consolidado, verificamos se temos os DataFrames individuais para consolidar
    #         print("\nConsolidando resultados dos três métodos para visualização...")
            
    #         # Converter os DataFrames para o formato necessário e adicionar flags
    #         dfs_processados = []
            
    #         if df_empirico is not None and not df_empirico.empty:
    #             df_emp = df_empirico.copy()
    #             df_emp['metodo'] = 'empirico'
    #             df_emp['flag_empirico'] = True
    #             df_emp['flag_tukey'] = False
    #             df_emp['flag_z_score'] = False
    #             df_emp['data_venda'] = pd.to_datetime(df_emp['data_venda'])
    #             dfs_processados.append(df_emp)
                
    #         if df_tukey is not None and not df_tukey.empty:
    #             df_tuk = df_tukey.copy()
    #             df_tuk['metodo'] = 'tukey'
    #             df_tuk['flag_empirico'] = False
    #             df_tuk['flag_tukey'] = True
    #             df_tuk['flag_z_score'] = False
    #             df_tuk['data_venda'] = pd.to_datetime(df_tuk['data_venda'])
    #             dfs_processados.append(df_tuk)
                
    #         # if df_zscore is not None and not df_zscore.empty:
    #         #     df_zsc = df_zscore.copy()
    #         #     df_zsc['metodo'] = 'zscore'
    #         #     df_zsc['flag_empirico'] = False
    #         #     df_zsc['flag_tukey'] = False
    #         #     df_zsc['flag_z_score'] = True
    #         #     df_zsc['data_venda'] = pd.to_datetime(df_zsc['data_venda'])
    #         #     dfs_processados.append(df_zsc)
                
    #         if not dfs_processados:
    #             print("Nenhum DataFrame com resultados foi fornecido. Impossível gerar visualizações.")
    #             return None
                
    #         # Consolidar os DataFrames
    #         df_consolidado = pd.concat(dfs_processados, ignore_index=True)
    #     else:
    #         # Se já temos o consolidado, apenas garantir que data_venda é datetime
    #         df_consolidado = df_consolidado.copy()
    #         df_consolidado['data_venda'] = pd.to_datetime(df_consolidado['data_venda'])
        
    #     # Garantir que temos dados de vendas preparados
    #     if df_vendas_preparado is None or df_vendas_preparado.empty:
    #         print("DataFrame de vendas preparado está vazio. Impossível gerar visualizações.")
    #         return None
            
    #     # Garantir que data_venda está no formato correto
    #     df_vendas_preparado = df_vendas_preparado.copy()
    #     df_vendas_preparado['data_venda'] = pd.to_datetime(df_vendas_preparado['data_venda'])
            
    #     # Colocar id_produto de ambos para integer64
    #     df_consolidado['id_produto'] = pd.to_numeric(df_consolidado['id_produto'], errors='coerce').astype('Int64')
    #     df_vendas_preparado['id_produto'] = pd.to_numeric(df_vendas_preparado['id_produto'], errors='coerce').astype('Int64')
        
    #     # Obter produtos únicos com vendas atípicas
    #     produtos_atipicos = df_consolidado['id_produto'].unique()
        
    #     if len(produtos_atipicos) == 0:
    #         print("Nenhum produto com vendas atípicas encontrado.")
    #         return None
            
    #     # Identificar produtos únicos
    #     produtos_unicos = df_consolidado[['id_produto', 'produto']].drop_duplicates()
    #     print(f"Encontrados {len(produtos_unicos)} produtos com vendas atípicas")
        
    #     # Configurar estilo dos gráficos
    #     sns.set(style="whitegrid")
    #     plt.rcParams.update({'figure.figsize': (14, 8),
    #                     'font.size': 12,
    #                     'axes.titlesize': 16,
    #                     'axes.labelsize': 14})
        
    #     # Contadores para estatísticas
    #     contador_empirico = 0
    #     contador_tukey = 0
    #     # contador_zscore = 0
    #     contador_combinados = 0
        
    #     # Para cada produto, gerar uma visualização com o histórico completo
    #     for _, row in produtos_unicos.iterrows():
    #         id_produto = row['id_produto']
    #         nome_produto = row['produto']
            
    #         print(f"Gerando visualização para o produto: {nome_produto} (ID: {id_produto})")
            
    #         # Filtrar histórico do produto diretamente do df_vendas_preparado
    #         historico_produto = df_vendas_preparado[df_vendas_preparado['id_produto'] == id_produto].copy()
            
    #         # Verificar se temos dados históricos
    #         if historico_produto.empty:
    #             print(f"Sem dados históricos para o produto {nome_produto}. Pulando...")
    #             continue
                
    #         # Filtrar dados das anomalias deste produto
    #         anomalias_produto = df_consolidado[df_consolidado['id_produto'] == id_produto].copy()
            
    #         # Verificar quais métodos detectaram anomalias para este produto
    #         metodos_produto = []
    #         if 'flag_empirico' in anomalias_produto.columns and anomalias_produto['flag_empirico'].sum() > 0:
    #             metodos_produto.append('empirico')
    #         if 'flag_tukey' in anomalias_produto.columns and anomalias_produto['flag_tukey'].sum() > 0:
    #             metodos_produto.append('tukey')
    #         # if 'flag_z_score' in anomalias_produto.columns and anomalias_produto['flag_z_score'].sum() > 0:
    #         #     metodos_produto.append('zscore')
            
    #         # Determinar em qual pasta este produto será salvo
    #         if len(metodos_produto) > 1:
    #             diretorio_produto = diretorio_combinado
    #             contador_combinados += 1
    #         elif 'empirico' in metodos_produto:
    #             diretorio_produto = diretorio_empirico
    #             contador_empirico += 1
    #         elif 'tukey' in metodos_produto:
    #             diretorio_produto = diretorio_tukey
    #             contador_tukey += 1
    #         # elif 'zscore' in metodos_produto:
    #         #     diretorio_produto = diretorio_zscore
    #         #     contador_zscore += 1
    #         else:
    #             # Caso excepcional, não deveria ocorrer
    #             diretorio_produto = diretorio_visualizacoes

    #         # Criar figura
    #         fig, ax = plt.subplots(figsize=(14, 8))
            
    #         # Preparar dados para visualização - agrupar vendas por dia
    #         vendas_diarias = historico_produto.groupby('data_venda')['quantidade'].sum().reset_index()
            
    #         # Plotar a série temporal de vendas completa (linha de base)
    #         ax.plot(vendas_diarias['data_venda'], vendas_diarias['quantidade'], 
    #             marker='o', linestyle='-', color='gray', alpha=0.6, 
    #             markersize=4, label='Vendas Normais')
            
    #         # Preparar cores para cada método
    #         metodos = {
    #             'empirico': {'cor': 'red', 'marker': 'X', 'label': 'Método Empírico'},
    #             'tukey': {'cor': 'blue', 'marker': '^', 'label': 'Método Tukey (IQR)'},
    #             # 'zscore': {'cor': 'green', 'marker': 'D', 'label': 'Método Z-Score'}
    #         }
            
    #         # Adicionar informação adicional sobre métodos
    #         metodos_usados = []
    #         if 'flag_empirico' in anomalias_produto.columns and anomalias_produto['flag_empirico'].sum() > 0:
    #             metodos_usados.append("Empírico")
    #         if 'flag_tukey' in anomalias_produto.columns and anomalias_produto['flag_tukey'].sum() > 0:
    #             metodos_usados.append("Tukey (IQR)")
    #         # if 'flag_z_score' in anomalias_produto.columns and anomalias_produto['flag_z_score'].sum() > 0:
    #         #     metodos_usados.append("Modified Z-Score")
                
    #         # Destacar pontos atípicos por método
    #         for metodo, config in metodos.items():
    #             flag_col = f'flag_{metodo}' if metodo != 'zscore' else 'flag_z_score'
                
    #             if flag_col in anomalias_produto.columns:
    #                 pontos_atipicos = anomalias_produto[anomalias_produto[flag_col] == True]
                    
    #                 if not pontos_atipicos.empty:
    #                     # Plotar pontos atípicos
    #                     scatter = ax.scatter(
    #                         pontos_atipicos['data_venda'], 
    #                         pontos_atipicos['quantidade_atipica'],
    #                         color=config['cor'], 
    #                         marker=config['marker'], 
    #                         s=120, 
    #                         alpha=0.8,
    #                         label=f"{config['label']} ({len(pontos_atipicos)} eventos)"
    #                     )
                        
    #                     # Adicionar linhas verticais nas datas das vendas atípicas
    #                     for _, point in pontos_atipicos.iterrows():
    #                         ax.axvline(x=point['data_venda'], color=config['cor'], linestyle='--', alpha=0.3)
                            
    #                         # Anotação com quantidade
    #                         ax.annotate(
    #                             f"{int(point['quantidade_atipica'])}",
    #                             xy=(point['data_venda'], point['quantidade_atipica']),
    #                             xytext=(0, 8),
    #                             textcoords='offset points',
    #                             ha='center',
    #                             fontsize=10,
    #                             fontweight='bold',
    #                             bbox=dict(boxstyle='round,pad=0.2', fc='white', alpha=0.7, ec=config['cor'])
    #                         )
            
    #         # Obter informações adicionais sobre o produto
    #         info_texto = f"Estratégias utilizadas: {', '.join(metodos_usados)}\n"
            
    #         # Adicionar informação de estoque se disponível
    #         if 'estoque_atualizado' in anomalias_produto.columns and not anomalias_produto['estoque_atualizado'].isna().all():
    #             estoque = anomalias_produto['estoque_atualizado'].iloc[0]
    #             info_texto += f"Estoque atual: {estoque} unidades\n"
            
    #         # Adicionar cobertura de dias se disponível
    #         if 'cobertura_dias' in anomalias_produto.columns and not anomalias_produto['cobertura_dias'].isna().all():
    #             cobertura = anomalias_produto['cobertura_dias'].iloc[0]
    #             info_texto += f"Cobertura: {cobertura:.1f} dias\n"
            
    #         # Configurar título e rótulos
    #         ax.set_title(f"Histórico de Vendas - {nome_produto}", fontsize=16)
    #         ax.set_xlabel("Data", fontsize=14)
    #         ax.set_ylabel("Quantidade Vendida", fontsize=14)
            
    #         # Configurar formatação de datas no eixo x
    #         ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    #         ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    #         fig.autofmt_xdate(rotation=45)
            
    #         # Adicionar grade
    #         ax.grid(True, linestyle='--', alpha=0.6)
            
    #         # Adicionar legenda
    #         ax.legend(loc='upper right')
            
    #         # Adicionar caixa com informações
    #         props = dict(boxstyle='round', facecolor='lightyellow', alpha=0.7)
    #         ax.text(0.02, 0.97, info_texto, transform=ax.transAxes, fontsize=12,
    #                 verticalalignment='top', bbox=props)
            
    #         # Ajustar layout
    #         plt.tight_layout()
            
    #         # Salvar imagem no diretório correspondente
    #         nome_arquivo = f"produto_{id_produto}_historico_vendas_atipicas.png"
    #         caminho_arquivo = os.path.join(diretorio_produto, nome_arquivo)
    #         plt.savefig(caminho_arquivo, dpi=150)
    #         plt.close()
            
    #         print(f"Visualização salva em: {caminho_arquivo}")
        
    #     # Gerar um gráfico resumo dos produtos com mais ocorrências atípicas para cada método
    #     print("\nGerando visualizações resumo dos produtos com mais vendas atípicas por método...")
        
    #     # Para o método empírico
    #     if 'flag_empirico' in df_consolidado.columns and df_consolidado['flag_empirico'].sum() > 0:
    #         produtos_empirico = df_consolidado[df_consolidado['flag_empirico']].groupby(['id_produto', 'produto']).size().reset_index(name='total_atipicidades')
    #         gerar_grafico_resumo(produtos_empirico, "Método Empírico", diretorio_empirico)
        
    #     # Para o método Tukey
    #     if 'flag_tukey' in df_consolidado.columns and df_consolidado['flag_tukey'].sum() > 0:
    #         produtos_tukey = df_consolidado[df_consolidado['flag_tukey']].groupby(['id_produto', 'produto']).size().reset_index(name='total_atipicidades')
    #         gerar_grafico_resumo(produtos_tukey, "Método Tukey (IQR)", diretorio_tukey)
        
    #     # # Para o método Z-Score
    #     # if 'flag_z_score' in df_consolidado.columns and df_consolidado['flag_z_score'].sum() > 0:
    #     #     produtos_zscore = df_consolidado[df_consolidado['flag_z_score']].groupby(['id_produto', 'produto']).size().reset_index(name='total_atipicidades')
    #     #     gerar_grafico_resumo(produtos_zscore, "Método Modified Z-Score", diretorio_zscore)
        
    #     # Gráfico resumo geral
    #     produtos_contagem = df_consolidado.groupby(['id_produto', 'produto']).size().reset_index(name='total_atipicidades')
    #     gerar_grafico_resumo(produtos_contagem, "Todos os Métodos", diretorio_visualizacoes)
        
    #     # Imprimir estatísticas finais
    #     print(f"\nEstatísticas de produtos com vendas atípicas:")
    #     print(f"- Produtos detectados apenas pelo Método Empírico: {contador_empirico}")
    #     print(f"- Produtos detectados apenas pelo Método Tukey: {contador_tukey}")
    #     # print(f"- Produtos detectados apenas pelo Método Z-Score: {contador_zscore}")
    #     print(f"- Produtos detectados por múltiplos métodos: {contador_combinados}")
    #     print(f"- Total de produtos com vendas atípicas: {len(produtos_unicos)}")
        
    #     print(f"\nVisualização de produtos atípicos concluída para o cliente {nome_cliente}")
    #     print(f"Todas as visualizações foram salvas em: {diretorio_visualizacoes}")
        
    #     return diretorio_visualizacoes
    
    # def gerar_grafico_resumo(df_produtos, nome_metodo, diretorio):
    #     """
    #     Gera gráfico de barras com os produtos com mais vendas atípicas para um método específico
    #     """
    #     if df_produtos.empty:
    #         print(f"Sem dados para gerar gráfico resumo para {nome_metodo}")
    #         return
            
    #     # Pegar os top 15 produtos com mais atipicidades
    #     top_produtos = df_produtos.sort_values('total_atipicidades', ascending=False).head(15)
        
    #     plt.figure(figsize=(14, 10))
        
    #     # Criar gráfico de barras com os produtos com mais atipicidades
    #     barras = plt.barh(top_produtos['produto'], top_produtos['total_atipicidades'], color='darkred')
        
    #     # Adicionar valores no final das barras
    #     for i, v in enumerate(top_produtos['total_atipicidades']):
    #         plt.text(v + 0.1, i, str(v), va='center')
            
    #     plt.title(f'Top 15 Produtos com Mais Vendas Atípicas - {nome_metodo}', fontsize=16)
    #     plt.xlabel('Número de Ocorrências Atípicas', fontsize=14)
    #     plt.ylabel('Produto', fontsize=14)
    #     plt.grid(axis='x', linestyle='--', alpha=0.6)
        
    #     plt.tight_layout()
        
    #     # Salvar gráfico resumo
    #     caminho_resumo = os.path.join(diretorio, f"resumo_produtos_atipicos.png")
    #     plt.savefig(caminho_resumo, dpi=150)
    #     plt.close()
        
    #     print(f"Visualização resumo para {nome_metodo} salva em: {caminho_resumo}")

    def consolidar_resultados_analises(df_empirico=None, df_tukey=None, df_zscore=None):
        """
        Consolida os resultados dos três métodos de detecção de vendas atípicas em um único DataFrame.
        
        Args:
            df_empirico: DataFrame com resultados do método empírico
            df_tukey: DataFrame com resultados do método de Tukey (IQR)
            df_zscore: DataFrame com resultados do método Modified Z-Score
        
        Returns:
            DataFrame consolidado com os resultados dos três métodos
        """
        print(f"Recebido para consolidação:")
        print(f"- DataFrame Empírico: {'Não vazio' if df_empirico is not None and not df_empirico.empty else 'Vazio ou None'}")
        print(f"- DataFrame Tukey: {'Não vazio' if df_tukey is not None and not df_tukey.empty else 'Vazio ou None'}")
        # print(f"- DataFrame Z-Score: {'Não vazio' if df_zscore is not None and not df_zscore.empty else 'Vazio ou None'}")

        print("\nConsolidando resultados dos três métodos...")
        
        # Verificar se temos pelo menos um DataFrame com resultados
        if (df_empirico is None or df_empirico.empty) and \
        (df_tukey is None or df_tukey.empty):
            print("Nenhum resultado disponível para consolidar.")
            return pd.DataFrame()
        
        # Timestamp atual para registrar o momento da análise
        datetime_analise = datetime.now()
        
        # Normalizar os DataFrames e adicionar flags
        dfs_processados = []
        
        if df_empirico is not None and not df_empirico.empty:
            df_emp = df_empirico.copy()
            df_emp['metodo'] = 'empirico'
            df_emp['flag_empirico'] = True
            df_emp['flag_tukey'] = False
            # df_emp['flag_z_score'] = False
            
            # Garantir que as colunas de identificação sejam strings para comparação
            df_emp['id_venda'] = df_emp['id_venda'].astype(str)
            # Preservar id_produto como numérico para evitar problemas de tipagem
            df_emp['id_produto'] = pd.to_numeric(df_emp['id_produto'], errors='coerce')
            df_emp['data_venda'] = pd.to_datetime(df_emp['data_venda']).dt.strftime('%Y-%m-%d')
            
            dfs_processados.append(df_emp)
            print(f"DataFrame Empírico processado: {len(df_emp)} registros")
        
        if df_tukey is not None and not df_tukey.empty:
            df_tuk = df_tukey.copy()
            df_tuk['metodo'] = 'tukey'
            df_tuk['flag_empirico'] = False
            df_tuk['flag_tukey'] = True
            # df_tuk['flag_z_score'] = False
            
            # Garantir que as colunas de identificação sejam strings para comparação
            df_tuk['id_venda'] = df_tuk['id_venda'].astype(str)
            # Preservar id_produto como numérico para evitar problemas de tipagem
            df_tuk['id_produto'] = pd.to_numeric(df_tuk['id_produto'], errors='coerce')
            df_tuk['data_venda'] = pd.to_datetime(df_tuk['data_venda']).dt.strftime('%Y-%m-%d')
            
            dfs_processados.append(df_tuk)
            print(f"DataFrame Tukey processado: {len(df_tuk)} registros")
        
        # if df_zscore is not None and not df_zscore.empty:
        #     df_zsc = df_zscore.copy()
        #     df_zsc['metodo'] = 'zscore'
        #     df_zsc['flag_empirico'] = False
        #     df_zsc['flag_tukey'] = False
        #     df_zsc['flag_z_score'] = True
            
        #     # Garantir que as colunas de identificação sejam strings para comparação
        #     df_zsc['id_venda'] = df_zsc['id_venda'].astype(str)
        #     df_zsc['id_produto'] = df_zsc['id_produto'].astype(str)
        #     df_zsc['data_venda'] = pd.to_datetime(df_zsc['data_venda']).dt.strftime('%Y-%m-%d')
            
        #     dfs_processados.append(df_zsc)
        #     print(f"DataFrame Z-Score processado: {len(df_zsc)} registros")
        
        if not dfs_processados:
            print("Nenhum DataFrame válido para processar.")
            return pd.DataFrame()
        
        # Identificar todas as vendas únicas usando um conjunto
        vendas_unicas = set()
        
        for df in dfs_processados:
            for _, row in df.iterrows():
                chave = (row['id_venda'], row['id_produto'], row['data_venda'])
                vendas_unicas.add(chave)
        
        print(f"Total de vendas únicas identificadas: {len(vendas_unicas)}")
        
        # Criar registros consolidados
        registros_consolidados = []
        
        for id_venda, id_produto, data_venda in vendas_unicas:
            # Inicializar registro base
            registro = {
                'datetime_analise': datetime_analise,
                'id_venda': id_venda,
                'id_produto': id_produto,
                'data_venda': data_venda,
                'flag_empirico': False,
                'flag_tukey': False,
                'metodos_detectaram': []
            }
            
            # Buscar dados em cada DataFrame
            dados_encontrados = False
            
            for df in dfs_processados:
                # Buscar a venda específica neste DataFrame
                mask = (df['id_venda'] == id_venda) & \
                    (df['id_produto'] == id_produto) & \
                    (df['data_venda'] == data_venda)
                
                if mask.any():
                    row = df[mask].iloc[0]
                    dados_encontrados = True
                    
                    # Identificar qual método detectou esta venda
                    metodo = row['metodo']
                    registro['metodos_detectaram'].append(metodo)
                    
                    # Setar as flags apropriadas
                    if metodo == 'empirico':
                        registro['flag_empirico'] = True
                    elif metodo == 'tukey':
                        registro['flag_tukey'] = True
                    # elif metodo == 'zscore':
                    #     registro['flag_z_score'] = True
                    
                    # Copiar campos comuns (apenas na primeira vez ou se ainda não existem)
                    campos_comuns = ['quantidade_atipica', 'id_cliente', 'cliente', 
                                'id_loja', 'loja', 'produto', 'estoque_atualizado', 
                                'cobertura_dias']
                    
                    for campo in campos_comuns:
                        if campo in row and campo not in registro:
                            registro[campo] = row[campo]
                    
                    # Copiar campos específicos do método com prefixo
                    if metodo == 'empirico':
                        if 'media_vendas' in row:
                            registro['media_vendas_empirico'] = row['media_vendas']
                        if 'razao_atipicidade' in row:
                            registro['razao_atipicidade'] = row['razao_atipicidade']
                    
                    elif metodo == 'tukey':
                        campos_tukey = ['Q1', 'Q3', 'IQR', 'limite_inferior', 'limite_superior', 
                                    'tipo_anomalia', 'desvio']
                        for campo in campos_tukey:
                            if campo in row:
                                registro[f'{campo}_tukey'] = row[campo]
                    
                    # elif metodo == 'zscore':
                    #     campos_zscore = ['mediana', 'mad', 'modified_zscore', 'tipo_anomalia']
                    #     for campo in campos_zscore:
                    #         if campo in row:
                    #             registro[f'{campo}_zscore'] = row[campo]
            
            # Adicionar informações consolidadas apenas se atender aos critérios adicionais
            if dados_encontrados:
                # Verificar os critérios: quantidade_atipica >= min_atipicidade e cobertura_dias <= valor_ruptura_estoque
                quantidade_ok = 'quantidade_atipica' in registro and registro['quantidade_atipica'] >= min_atipicidade
                cobertura_ok = 'cobertura_dias' in registro and registro['cobertura_dias'] <= valor_ruptura_estoque
                
                if quantidade_ok and cobertura_ok:
                    registro['qtd_metodos_detectaram'] = len(registro['metodos_detectaram'])
                    registro['metodos_str'] = ', '.join(sorted(registro['metodos_detectaram']))
                    registro['atipica_algum_metodo'] = True
                    
                    registros_consolidados.append(registro)
        
        # Criar DataFrame final
        if registros_consolidados:
            df_consolidado = pd.DataFrame(registros_consolidados)
            
            # Verificar se as flags estão sendo definidas corretamente
            print(f"\nResumo das flags no DataFrame consolidado:")
            print(f"- Flag Empírico True: {df_consolidado['flag_empirico'].sum()}")
            print(f"- Flag Tukey True: {df_consolidado['flag_tukey'].sum()}")
            # print(f"- Flag Z-Score True: {df_consolidado['flag_z_score'].sum()}")
            # print(f"- Pelo menos um método True: {df_consolidado['atipica_algum_metodo'].sum()}")
            
            return df_consolidado
        else:
            print("Nenhum registro consolidado foi criado.")
            return pd.DataFrame() 
        
    try:
        print("Obtendo dados base para análise...")
        df_vendas, df_venda_itens, df_clientes, df_produtos, df_estoque, df_compras, df_lojas = obter_dados_base(conn)
        
        print("Preparando dados de vendas...")
        df_vendas_preparado = preparar_dados_vendas(df_vendas, df_venda_itens)
        
        if not analise_varejo:
            print("Analise de vendas atípicas para ATACADO")
            print("Identificando produtos com vendas atípicas...")
            anomalias = identificar_produtos_anomalos(df_vendas_preparado)
            
            print("Gerando relatório de vendas atípicas...")
            df_resultados = gerar_relatorio_vendas_atipicas(anomalias, df_produtos, df_clientes, df_estoque, df_compras, df_lojas)
            
            ###############################
            # Salvar relatório
            ###############################
            # Exportar resultados para CSV
            # df_resultados.to_csv(os.path.join(diretorio_vendas_atipicas, f"analise_vendas_atipicas.csv"), index=False)
            # print(f"Arquivo salvo para o cliente {nome_cliente} em: {diretorio_vendas_atipicas}")
            print(f"Colunas disponíveis: {', '.join(df_resultados.columns)}")

            # Exportar para banco
            exportar_resultados(df_resultados, analise_varejo=False)
            
            print("Análise completa!")

        else:
            print("Analise de vendas atípicas para VAREJISTA")
            # print(f"Usando método empírico com período de {tempo_metodo_empirico_em_dias} dias")
            
            # # Preparar dados para análise
            # df_anomalias, anomalias_encontradas = identificar_anomalias_metodo_empirico(
            #     df_vendas_preparado, tempo_metodo_empirico_em_dias)
            
            # if anomalias_encontradas:
            #     # Formatar resultados para o relatório
            #     df_resultados_empirico = pd.DataFrame()
                
            #     # Agrupar anomalias por produto
            #     for id_produto in df_anomalias['id_produto'].unique():
            #         anomalias_produto = df_anomalias[df_anomalias['id_produto'] == id_produto]
                    
            #         # Obter informações do produto
            #         produto_info = df_produtos[df_produtos['id_produto'] == id_produto]
            #         produto = produto_info['nome'].iloc[0] if not produto_info.empty else f"Produto {id_produto}"
                    
            #         # Obter estoque atual
            #         estoque_info = df_estoque[df_estoque['id_produto'] == id_produto]
            #         estoque = estoque_info['estoque'].iloc[0] if not estoque_info.empty else 0
                    
            #         # Obter cobertura de dias do produto
            #         cobertura_info = df_compras[df_compras['id_produto'] == id_produto]
            #         cobertura_dias = cobertura_info['cobertura_dias'].iloc[0] if not cobertura_info.empty else 0
                    
            #         # Criar registros para cada anomalia do produto
            #         for _, anomalia in anomalias_produto.iterrows():
            #             id_cliente = anomalia['id_cliente']
            #             id_loja = anomalia['id_loja']
                        
            #             # Obter informações do cliente
            #             cliente_info = df_clientes[df_clientes['id_cliente'] == id_cliente]
            #             cliente = cliente_info['nome'].iloc[0] if not cliente_info.empty else "Cliente não identificado"
                        
            #             # Obter informações da loja
            #             loja_info = df_lojas[df_lojas['id_loja'] == id_loja]
            #             loja = loja_info['nome'].iloc[0] if not loja_info.empty else "Loja não identificada"
                        
            #             # Criar registro para adicionar ao DataFrame de resultados
            #             registro = {
            #                 'data_venda': anomalia['data_venda'],
            #                 'quantidade_atipica': anomalia['quantidade'],
            #                 'id_cliente': id_cliente,
            #                 'cliente': cliente,
            #                 'id_venda': anomalia['id_venda'],
            #                 'id_loja': id_loja,
            #                 'loja': loja,
            #                 'id_produto': id_produto,
            #                 'produto': produto,
            #                 'estoque_atualizado': estoque,
            #                 'cobertura_dias': cobertura_dias,
            #                 'media_vendas': round(anomalia['media_referencia'], 2),
            #                 'razao_atipicidade': round(anomalia['razao'], 2)
            #             }
                        
            #             # Adicionar ao DataFrame de resultados somente se atender aos critérios
            #             if registro['quantidade_atipica'] >= min_atipicidade and registro['cobertura_dias'] <= valor_ruptura_estoque:
            #                 df_resultados_empirico = pd.concat([df_resultados_empirico, pd.DataFrame([registro])], 
            #                                         ignore_index=True)
                
            #     print(f"Encontradas {len(df_resultados_empirico)} vendas atípicas pelo método empírico")
                
            #     ###############################
            #     # Salvar relatório
            #     ###############################
            #     # Exportar resultados para CSV
            #     df_resultados_empirico.to_csv(os.path.join(diretorio_vendas_atipicas, f"analise_vendas_atipicas_metodo_empirico.csv"), index=False)
            #     print(f"Arquivo do metodo empirico salvo para o cliente {nome_cliente}")
            #     # print(f"Colunas disponíveis: {', '.join(df_resultados_tukey.columns)}")

            #     # Exportar para banco
            #     # df_resultados_tukey(df_resultados_tukey)
            # else:
            #     print("Nenhuma venda atípica encontrada pelo método empírico")

            print("Realizando análise estatística de vendas atípicas com Tukey (IQR)...")
            # Preparar dados para análise
            df_anomalias, anomalias_encontradas = identificar_anomalias_metodo_estatistico(df_vendas_preparado)
            
            if anomalias_encontradas:
                # Formatar resultados para o relatório
                df_resultados_tukey = pd.DataFrame()
                
                # Agrupar anomalias por produto
                for id_produto in df_anomalias['id_produto'].unique():
                    anomalias_produto = df_anomalias[df_anomalias['id_produto'] == id_produto]
                    
                    # Obter informações do produto
                    produto_info = df_produtos[df_produtos['id_produto'] == id_produto]
                    produto = produto_info['nome'].iloc[0] if not produto_info.empty else f"Produto {id_produto}"
                    
                    # Obter estoque atual
                    estoque_info = df_estoque[df_estoque['id_produto'] == id_produto]
                    estoque = estoque_info['estoque'].iloc[0] if not estoque_info.empty else 0
                    
                    # Obter cobertura de dias do produto
                    cobertura_info = df_compras[df_compras['id_produto'] == id_produto]
                    cobertura_dias = cobertura_info['cobertura_dias'].iloc[0] if not cobertura_info.empty else 0
                    
                    # Calcular a média de vendas para o produto (mesmo cálculo usado para atacadista)
                    vendas_produto_todas = df_vendas_preparado[df_vendas_preparado['id_produto'] == id_produto]
                    media_vendas = vendas_produto_todas['quantidade'].mean()
                    
                    # Criar registros para cada anomalia do produto                    
                    for _, anomalia in anomalias_produto.iterrows():
                        # Garantir que id_cliente seja None se estiver vazio ou não existir
                        if 'id_cliente' in anomalia and pd.notna(anomalia['id_cliente']) and anomalia['id_cliente'] != '':
                            id_cliente = anomalia['id_cliente']
                            # Obter informações do cliente
                            cliente_info = df_clientes[df_clientes['id_cliente'] == id_cliente]
                            cliente = cliente_info['nome'].iloc[0] if not cliente_info.empty else "Cliente não identificado"
                        else:
                            id_cliente = None
                            cliente = "Cliente não identificado"
                            
                        id_loja = anomalia['id_loja']
                        
                        # Obter informações da loja
                        loja_info = df_lojas[df_lojas['id_loja'] == id_loja]
                        loja = loja_info['nome'].iloc[0] if not loja_info.empty else "Loja não identificada"

                        #date time apenas com a data
                        anomalia['data_venda'] = anomalia['data_venda'].date()

                        #id_produto deve ser tipo integer para o banco
                        id_produto = int(id_produto)
                        
                        # Criar registro para adicionar ao DataFrame de resultados
                        registro = {
                            'data_venda': anomalia['data_venda'],
                            'quantidade_atipica': anomalia['quantidade'],
                            'id_cliente': id_cliente,
                            'cliente': cliente,
                            'id_venda': anomalia['id_venda'],
                            'id_loja': id_loja,
                            'loja': loja,
                            'id_produto': id_produto,
                            'produto': produto,
                            'estoque_atualizado': estoque,
                            'cobertura_dias': cobertura_dias,
                            'media_vendas': round(media_vendas, 2),  # Adicionando a média de vendas do produto
                            # 'Q1': round(anomalia['Q1'], 2),
                            # 'Q3': round(anomalia['Q3'], 2),
                            # 'IQR': round(anomalia['IQR'], 2),
                            # 'limite_inferior': round(anomalia['limite_inferior'], 2),
                            # 'limite_superior': round(anomalia['limite_superior'], 2),
                            'tipo_atipicidade': anomalia['tipo_anomalia'],
                            'valor_desvio': round(anomalia['desvio'], 2)
                        }
                        
                        # Adicionar ao DataFrame de resultados somente se atender aos critérios
                        if registro['quantidade_atipica'] >= min_atipicidade and registro['cobertura_dias'] <= valor_ruptura_estoque:
                            df_resultados_tukey = pd.concat([df_resultados_tukey, pd.DataFrame([registro])], 
                                                    ignore_index=True)
                
                print(f"Encontradas {len(df_resultados_tukey)} vendas atípicas pelo método estatístico IQR")
                
                ###############################
                # Salvar relatório
                ###############################
                # Exportar resultados para CSV
                # df_resultados_tukey.to_csv(os.path.join(diretorio_vendas_atipicas, f"analise_vendas_atipicas_metodo_estatistico_tukey.csv"), index=False)
                # print(f"Arquivo do metodo tukey salvo para o cliente {nome_cliente}")
                # print(f"Colunas disponíveis: {', '.join(df_resultados_tukey.columns)}")

                # Exportar para banco
                exportar_resultados(df_resultados_tukey, analise_varejo=True)
            else:
                print("Nenhuma venda atípica encontrada pelo método estatístico IQR")

            # # Preparar dados para análise com o método Modified Z-Score
            # print(f"Aplicando método estatístico Modified Z-Score com período de análise de {tempo_analise_estatistico} dias...")
            # df_anomalias, anomalias_encontradas = identificar_anomalias_metodo_modified_zscore(df_vendas_preparado, tempo_analise_estatistico)

            # if anomalias_encontradas:
            #     # Formatar resultados para o relatório
            #     df_resultados_zscore = pd.DataFrame()
                
            #     # Agrupar anomalias por produto
            #     for id_produto in df_anomalias['id_produto'].unique():
            #         anomalias_produto = df_anomalias[df_anomalias['id_produto'] == id_produto]
                    
            #         # Obter informações do produto
            #         produto_info = df_produtos[df_produtos['id_produto'] == id_produto]
            #         produto = produto_info['nome'].iloc[0] if not produto_info.empty else f"Produto {id_produto}"
                    
            #         # Obter estoque atual
            #         estoque_info = df_estoque[df_estoque['id_produto'] == id_produto]
            #         estoque = estoque_info['estoque'].iloc[0] if not estoque_info.empty else 0
                    
            #         # Obter cobertura de dias do produto
            #         cobertura_info = df_compras[df_compras['id_produto'] == id_produto]
            #         cobertura_dias = cobertura_info['cobertura_dias'].iloc[0] if not cobertura_info.empty else 0
                    
            #         # Criar registros para cada anomalia do produto
            #         for _, anomalia in anomalias_produto.iterrows():
            #             id_cliente = anomalia['id_cliente']
            #             id_loja = anomalia['id_loja']
                        
            #             # Obter informações do cliente
            #             cliente_info = df_clientes[df_clientes['id_cliente'] == id_cliente]
            #             cliente = cliente_info['nome'].iloc[0] if not cliente_info.empty else "Cliente não identificado"
                        
            #             # Obter informações da loja
            #             loja_info = df_lojas[df_lojas['id_loja'] == id_loja]
            #             loja = loja_info['nome'].iloc[0] if not loja_info.empty else "Loja não identificada"
                        
            #             # Criar registro para adicionar ao DataFrame de resultados
            #             registro = {
            #                 'data_venda': anomalia['data_venda'],
            #                 'quantidade_atipica': anomalia['quantidade'],
            #                 'id_cliente': id_cliente,
            #                 'cliente': cliente,
            #                 'id_venda': anomalia['id_venda'],
            #                 'id_loja': id_loja,
            #                 'loja': loja,
            #                 'id_produto': id_produto,
            #                 'produto': produto,
            #                 'estoque_atualizado': estoque,
            #                 'cobertura_dias': cobertura_dias,
            #                 'mediana': round(anomalia['mediana'], 2),
            #                 'mad': round(anomalia['mad'], 2),
            #                 'modified_zscore': round(anomalia['modified_zscore'], 2),
            #                 'tipo_anomalia': anomalia['tipo_anomalia']
            #             }
                        
            #             # Adicionar ao DataFrame de resultados
            #             df_resultados_zscore = pd.concat([df_resultados_zscore, pd.DataFrame([registro])], 
            #                                     ignore_index=True)
                
            #     print(f"Encontradas {len(df_resultados_zscore)} vendas atípicas pelo método Modified Z-Score")
                
            #     ###############################
            #     # Salvar relatório
            #     ###############################
            #     # Exportar resultados para CSV
            #     # df_resultados_zscore.to_csv(os.path.join(diretorio_vendas_atipicas, f"analise_vendas_atipicas_metodo_modified_zscore.csv"), index=False)
            #     print(f"Arquivo do z-score mediana salvo para o cliente {nome_cliente}")
                
            #     # Exportar para banco
            #     # exportar_resultados(df_resultados_zscore)
            # else:
            #     print("Nenhuma venda atípica encontrada pelo método Modified Z-Score")

            #Consolidar os resultados dos três métodos
            # print("\nConsolidando resultados dos três métodos de análise...")
            # df_consolidado = consolidar_resultados_analises(
            #     # df_empirico=df_resultados_empirico if 'df_resultados_empirico' in locals() else None,
            #     df_tukey=df_resultados_tukey if 'df_resultados_tukey' in locals() else None
            # )

            # arquivo_consolidado = os.path.join(diretorio_vendas_atipicas, f"analise_vendas_atipicas_TUKEY.csv")
            # df_resultados_tukey.to_csv(arquivo_consolidado, index=False)
            # print(f"Arquivo consolidado salvo para o cliente {nome_cliente}")
            
            # Exportar para o banco o df consolidado
            # exportar_resultados(df_consolidado)

            # Gerar visualizações atemporais dos produtos atípicos após a análise
            # print("\nGerando visualizações atemporais dos produtos com vendas atípicas...")
            # diretorio_visualizacoes = visualizar_produtos_atipicos(
            #     nome_cliente,
            #     df_vendas_preparado=df_vendas_preparado,
            #     # df_empirico=df_resultados_empirico if 'df_resultados_empirico' in locals() else None,
            #     df_tukey=df_resultados_tukey if 'df_resultados_tukey' in locals() else None,
            #     # df_consolidado=df_consolidado if 'df_consolidado' in locals() else None
            # )
            # print(f"Visualizações salvas em: {diretorio_visualizacoes}")
        
    except Exception as e:
        print(f"Erro durante a análise: {e}")
        
    finally:
        # Fechar conexão com banco de dados
        if conn:
            conn.close()
            print("Conexão com banco de dados encerrada.")

if __name__ == "__main__":
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Gera relatórios de vendas atípicas para um cliente específico')
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
            gerar_analise_vendas_atipicas(cliente)
        print("\nProcessamento de todos os clientes concluído!")
    else:
        # Executar a geração de relatórios apenas para o cliente especificado
        gerar_analise_vendas_atipicas(args.cliente)

"""
Para executar um cliente específico, use o comando:
python analise_vendas_atipicas.py nome_do_cliente

Para executar para todos os clientes, use o comando:
python analise_vendas_atipicas.py todos

Para executar para todos os clientes sem especificar argumentos, use:
python analise_vendas_atipicas.py
"""