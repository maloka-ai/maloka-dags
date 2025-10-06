"""
Módulo para gerenciamento de operações de atualização no banco de dados.

Este módulo fornece funções para inserir e atualizar dados em bancos PostgreSQL,
com otimizações para grandes volumes de dados através de processamento paralelo.
"""

import os
import sys
import warnings
from datetime import datetime
from multiprocessing import Pool, cpu_count
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import connection

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

# Configuração de avisos
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


def _determinar_tipo_sql(dtype: np.dtype) -> str:
    """
    Determina o tipo SQL correspondente para um tipo de dados pandas/numpy.
    
    Args:
        dtype: Tipo de dados do pandas/numpy
        
    Returns:
        String com o tipo SQL correspondente
    """
    dtype_str = str(dtype)
    if 'int64' in dtype_str:
        return 'BIGINT'
    elif 'int' in dtype_str:
        return 'INTEGER'
    elif 'float' in dtype_str:
        return 'NUMERIC'
    elif 'datetime' in dtype_str:
        return 'DATETIME'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        return 'TEXT'

def processar_lote(args: Tuple[List[Tuple], Dict[str, Any], str, int]) -> Tuple[int, Optional[str]]:
    """
    Processa lotes de dados para inserção paralela no banco de dados.

    Args:
        args: Tupla contendo os argumentos:
            - lote_dados: Lista de tuplas com os dados a serem inseridos
            - db_config: Configurações de conexão com o banco de dados
            - query: Query SQL para inserção de dados
            - start_index: Índice inicial do lote para acompanhamento

    Returns:
        Tupla contendo:
            - Quantidade de registros inseridos com sucesso
            - Mensagem de erro (None se não houver erros)
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
        
        return len(lote_dados), None
    except Exception as e:
        return 0, str(e)

def inserir_dataframe_postgres(
    df: pd.DataFrame, 
    db_config: Dict[str, Any], 
    schema: str, 
    tabela: str, 
    criar_indices: bool = False, 
    colunas_indice: Optional[List[str]] = None,
    batch_size: int = 10000
) -> bool:
    """
    Insere um DataFrame do pandas em uma tabela PostgreSQL de forma otimizada.
    
    Esta função realiza a inserção paralela de grandes volumes de dados,
    criando a tabela se não existir e gerenciando índices para otimizar o desempenho.
    
    Args:
        df: DataFrame a ser inserido no banco de dados
        db_config: Dicionário com configurações do banco de dados
            (host, database, user, password, port)
        schema: Nome do esquema no PostgreSQL
        tabela: Nome da tabela no PostgreSQL
        criar_indices: Se True, cria índices após inserção
        colunas_indice: Lista de colunas para criar índices
        batch_size: Tamanho do lote para inserção em paralelo
    
    Returns:
        True se a inserção foi bem-sucedida, False caso contrário
        
    Raises:
        Exception: Qualquer erro durante o processo de inserção
    """
    
    conn: Optional[connection] = None
    
    try:
        # Conectar ao PostgreSQL
        print("\nConectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Nome completo da tabela
        tabela_completa = f"{schema}.{tabela}"
        
        # Verificar se a tabela já existe
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_name=%s AND table_schema=%s
            )
        """, (tabela, schema))
        tabela_existe = cursor.fetchone()[0]
        
        if tabela_existe:
            print(f"Tabela {tabela_completa} já existe. Verificando colunas...")
            
            # Verificar colunas existentes
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name=%s AND table_schema=%s
            """, (tabela, schema))
            colunas_existentes = [row[0] for row in cursor.fetchall()]
            
            # Adicionar colunas que não existem
            for coluna in df.columns:
                if coluna.lower() not in [col.lower() for col in colunas_existentes]:
                    print(f"Adicionando nova coluna: {coluna}")
                    
                    # Determinar o tipo de dados SQL baseado no tipo pandas
                    tipo = _determinar_tipo_sql(df[coluna].dtype)
                    
                    try:
                        cursor.execute(f'ALTER TABLE {tabela_completa} ADD COLUMN "{coluna}" {tipo}')
                        conn.commit()
                        print(f"Coluna {coluna} adicionada com sucesso!")
                    except Exception as e:
                        print(f"Erro ao adicionar coluna {coluna}: {e}")
                        conn.rollback()
            
            # Limpar dados existentes
            print(f"Limpando dados existentes da tabela {tabela_completa}...")
            cursor.execute(f"TRUNCATE TABLE {tabela_completa}")
            conn.commit()
            
        else:
            print(f"Criando tabela {tabela_completa}...")
            
            # Criar schema se não existir
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Definir tipos de dados para cada coluna
            colunas = []
            for coluna, dtype in df.dtypes.items():
                tipo = _determinar_tipo_sql(dtype)
                colunas.append(f'"{coluna}" {tipo}')
            
            # Criar tabela UNLOGGED para inserção rápida
            # Criar tabela otimizada para inserção rápida
            create_table_query = f"""
            CREATE UNLOGGED TABLE {tabela_completa} (
                {", ".join(colunas)}
            )
            """
            cursor.execute(create_table_query)
            
            # Definir fator de preenchimento para otimização
            cursor.execute(f"ALTER TABLE {tabela_completa} SET (fillfactor = 90)")
            conn.commit()
            print(f"Tabela {tabela_completa} criada com sucesso!")
        
        # Preparar colunas para inserção
        colunas_insert = [f'"{col}"' for col in df.columns]
        
        # Otimizar para inserção
        print("Otimizando configurações para inserção rápida...")
        cursor.execute("SET maintenance_work_mem = '256MB'")
        cursor.execute("SET synchronous_commit = off")
        
        print(f"Inserindo {len(df)} registros na tabela {tabela_completa}...")
        
        # Preparar dados para inserção paralela
        df_upload = df.replace({np.nan: None})  # Converter NaN para None
            
        # Preparar query INSERT com placeholders
        placeholders = ", ".join(["%s"] * len(df_upload.columns))
        insert_query = f"""
        INSERT INTO {tabela_completa} ({", ".join(colunas_insert)})
        VALUES ({placeholders})
        """
            
        # Converter DataFrame em lista de tuplas para inserção
        valores = [tuple(row) for _, row in df_upload.iterrows()]
            
        # Configurar processamento paralelo otimizado
        num_cores = cpu_count()
        print(f"Usando {num_cores} núcleos para processamento paralelo")
            
        # Calcular tamanho de lote ideal baseado no número de núcleos
        records_per_core = max(1000, len(valores) // (num_cores * 2))
        batch_size = min(batch_size, records_per_core)
            
        # Criar lotes para processamento paralelo
        lotes = []
        for i in range(0, len(valores), batch_size):
            batch = valores[i:i+batch_size]
            lotes.append((batch, db_config, insert_query, i))
            
        # Processar em paralelo
        # Executar inserção paralela e monitorar desempenho
        start_time = datetime.now()
        print(f"Iniciando inserção paralela com {len(lotes)} lotes...")
            
        with Pool(processes=num_cores) as pool:
            # Processar lotes em paralelo
            resultados = pool.map(processar_lote, lotes)
                
            # Contabilizar resultados
            total_inseridos = sum(qtd for qtd, _ in resultados)
            erros = [erro for _, erro in resultados if erro]
                
            # Calcular e exibir estatísticas
            elapsed = (datetime.now() - start_time).total_seconds()
            taxa = total_inseridos / elapsed if elapsed > 0 else 0
            print(f"Inseridos {total_inseridos} de {len(valores)} registros em {elapsed:.2f} segundos")
            print(f"Taxa de inserção: {taxa:.2f} registros/segundo")
                
            # Mostrar erros, se houver
            if erros:
                print(f"Ocorreram {len(erros)} erros durante a inserção")
                for erro in erros[:5]:  # Limitar a exibição aos 5 primeiros erros
                    print(f"- {erro}")
        
        # Restaurar configurações e otimizar para consultas
        print("Restaurando configurações e otimizando para consultas...")
        cursor.execute("SET maintenance_work_mem = '64MB'")
        cursor.execute("SET synchronous_commit = on")
        cursor.execute(f"ALTER TABLE {tabela_completa} SET LOGGED")
        
        # Criar índices se solicitado
        if criar_indices and colunas_indice:
            print("Criando índices...")
            for coluna in colunas_indice:
                if coluna in df.columns:
                    nome_indice = f"idx_{tabela}_{coluna}"
                    try:
                        cursor.execute(f'CREATE INDEX IF NOT EXISTS {nome_indice} ON {tabela_completa} ("{coluna}")')
                        print(f"Índice {nome_indice} criado com sucesso!")
                    except Exception as e:
                        print(f"Erro ao criar índice {nome_indice}: {e}")
        
        # Analisar tabela para otimizar estatísticas do planejador
        print("Analisando tabela para otimizar consultas...")
        cursor.execute(f"ANALYZE {tabela_completa}")
        conn.commit()
        
        # Finalização
        print(f"\n✅ Inserção concluída com sucesso! Total: {len(df)} registros em {tabela_completa}")
        
        # Liberação de recursos
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro ao inserir dados no banco: {e}")
        import traceback
        traceback.print_exc()
        
        # Garantir que a conexão seja fechada em caso de erro
        if conn is not None:
            conn.close()
        
        return False