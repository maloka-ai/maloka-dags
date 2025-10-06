from datetime import datetime
import pandas as pd
import dotenv
import os
import warnings
import psycopg2
from fpdf import FPDF

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

dotenv.load_dotenv()

add_config = {
    "database": "add",
    "produtoX": "LUVA DE VINIL SEM PO TAM M C/ 100 UND TALGE",
    "produtoY": "AGUA SANITARIA BRILUX 1L",
    "vendedor": "MARIA EDUARDA",
    "categoria": "DIVERSOS/FERRAGENS/UTILIDADES"
}

bibi_config = {
    "database": "bibicell",
    "produtoX": "FONE DE OUVIDO SEM FIO A GOLD V5.3",
    "produtoY": "FONE DE OUVIDO SEM FIO A GOLD V5.3",
    "vendedor": "INGRID MORAES FERNANDES",
    "categoria": "FONE DE OUVIDO"
}

beny_config = {
    "database": "beny",
    "produtoX": "IMPERMANTA ELASTIC 4MM DENVER TIII",
    "produtoY": "ARG CABRAL 20KG ACIII CZ INT/EXT",
    "vendedor": "RAIMUNDO CECILIO LEAL DE SOUSA",
    "categoria": "Armarios De Cozinha"
}

general_config = add_config
# Conectar ao banco
def executar_query(query):
    try:
        # Conectar ao PostgreSQL
        print("Executando query...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=general_config["database"],
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )

        # Executar a consulta
        df = pd.read_sql_query(query, conn)

        # Fechar a conexão
        conn.close()

        return df

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return None

# Função para gerar arquivo PDF com os resultados
def salvar_resultados_pdf(resultados, nome_arquivo):
    try:
        class PDF(FPDF):
            def header(self):
                # Logo (opcional)
                # self.image('logo.png', 10, 8, 33)
                # Título
                self.set_font('Arial', 'B', 11)
                self.cell(0, 10, f'Relatório de Consultas - {general_config["database"]}', 0, 1, 'C')
                # Data
                self.set_font('Arial', 'I', 10)
                self.cell(0, 10, f'Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}', 0, 1, 'R')
                # Linha
                self.line(10, 30, 200, 30)
                # Espaço depois do cabeçalho
                self.ln(5)

            def footer(self):
                # Posicionar a 1.5 cm do final
                self.set_y(-15)
                self.set_font('Arial', 'I', 8)
                # Adicionar número da página
                self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', 0, 0, 'C')
        
        pdf = PDF()
        pdf.alias_nb_pages()
        pdf.add_page()
        pdf.set_font("Arial", size=11)
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # Adicionar cada resultado
        for titulo, conteudo in resultados.items():
            if titulo == "#### Informações do Relatório ####":
                continue  # As informações já estão no cabeçalho
                
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, txt=titulo, ln=True)
            
            pdf.set_font("Arial", size=9)
            if isinstance(conteudo, list):
                for linha in conteudo:
                    # Verificar se a linha precisa ser dividida devido ao tamanho
                    linha_str = str(linha)
                    if len(linha_str) > 100:
                        # Dividir a linha em partes menores para caber na página
                        partes = [linha_str[i:i+100] for i in range(0, len(linha_str), 100)]
                        for parte in partes:
                            pdf.multi_cell(0, 5, parte, 0, 'L')
                    else:
                        pdf.multi_cell(0, 5, linha_str, 0, 'L')
            else:
                pdf.multi_cell(0, 5, str(conteudo), 0, 'L')
            
            pdf.ln(5)
        
        # Salvar PDF
        pdf.output(nome_arquivo)
        print(f"Resultados salvos em {nome_arquivo}")
        return True
    
    except Exception as e:
        print(f"Erro ao gerar PDF: {e}")
        return False

#####
# Liste os produtos com estoque negativo classificados como B na curva ABC
#####
print("\n")
print("\n")
print("#### PERGUNTA 1 ####")
query1 = """
    SELECT id_sku, nome_produto, ativo, excluido
    FROM maloka_core.analise_estoque ae 
    JOIN maloka_core.produto p on p.id_produto = ae.id_sku
    WHERE p.ativo = true and p.excluido = false and ae.curva_abc = 'B' and ae.estoque_atual < 0
"""
df_produtos_B = executar_query(query1)
print("Total de produtos com estoque negativo classificados como B na curva ABC:", df_produtos_B.shape[0])
print("Produtos com estoque negativo classificados como B na curva ABC: (15 primeiros)")
print(df_produtos_B.head(15))  # Mostrar apenas os 15 primeiros

#####
# Quais produtos da classe C na curva ABC estão negativos
#####
print("\n")
print("\n")
print("#### PERGUNTA 2 ####")
query2 = """
    SELECT id_sku, nome_produto, ativo, excluido
    FROM maloka_core.analise_estoque ae 
    JOIN maloka_core.produto p on p.id_produto = ae.id_sku
    WHERE p.ativo = true and p.excluido = false and ae.curva_abc = 'C' and ae.estoque_atual < 0
"""
df_produtos_C = executar_query(query2)
print("Total de produtos com estoque negativo classificados como C na curva ABC:", df_produtos_C.shape[0])
print("Produtos com estoque negativo classificados como C na curva ABC: (15 primeiros)")
print(df_produtos_C.head(15))  # Mostrar apenas os 15 primeiros

#####
# Liste produtos com estoque negativo classificado como C na curva ABC. Mostre além do estoque atual, o valor de venda de cada um.
#####
print("\n")
print("\n")
print("#### PERGUNTA 3 ####")
query3 = """
    SELECT id_sku, nome_produto, p.preco_venda
    FROM maloka_core.analise_estoque ae 
    JOIN maloka_core.produto p on p.id_produto = ae.id_sku
    WHERE p.ativo = true and p.excluido = false and ae.curva_abc = 'C' and ae.estoque_atual < 0
"""
df_produtos_C_valor = executar_query(query3)
print("Produtos com estoque negativo classificados como C na curva ABC com valor de venda: (15 primeiros)")
print(df_produtos_C_valor.head(15))  # Mostrar apenas os 15 primeiros

#####
# Quantas e quais são as categorias de produtos utilizadas?
#####
print("\n")
print("\n")
print("#### PERGUNTA 4 ####")
query4 = """
    SELECT 
        COUNT(*) as total_categorias,
        ARRAY_AGG(c.nome_categoria ORDER BY c.nome_categoria) as categorias
    FROM maloka_core.categoria c
"""
df_categorias = executar_query(query4)
print("Total de categorias de produtos:", df_categorias['total_categorias'][0])
print("Categorias de produtos:", df_categorias['categorias'][0])

####
# Quantas e quais são as segmentações de clientes?
#####
print("\n")
print("\n")
print("#### PERGUNTA 5 ####")
query5 = """
    SELECT 
        COUNT(DISTINCT s.segmento) as total_segmentos,
        ARRAY_AGG(DISTINCT s.segmento ORDER BY s.segmento) as segmentos
    FROM maloka_analytics.segmentacao s
    WHERE s.segmento IS NOT NULL
"""
df_segmentos = executar_query(query5)
print("Total de segmentações de clientes:", df_segmentos['total_segmentos'][0])
print("Segmentações de clientes:", df_segmentos['segmentos'][0])

####
# Compare o faturamento deste ano, até o último mês fechado, com o mesmo período no ano anterior
####
print("\n")
print("\n")
print("#### PERGUNTA 6 ####")
query6 = """
    WITH periodo_atual AS (
        SELECT 
            EXTRACT(MONTH FROM MAX(v.data_venda)) as ultimo_mes
        FROM maloka_core.venda v
        WHERE EXTRACT(YEAR FROM v.data_venda) = 2025
            AND v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
    ),
    faturamento_comparativo AS (
        SELECT
            EXTRACT(YEAR FROM v.data_venda) as ano,
            SUM(v.total_venda) as faturamento_total
        FROM maloka_core.venda v
        CROSS JOIN periodo_atual p
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND (
                (EXTRACT(YEAR FROM v.data_venda) = 2025 
                AND v.data_venda >= '2025-01-01' 
                AND v.data_venda < DATE_TRUNC('month', NOW()))
                OR
                (EXTRACT(YEAR FROM v.data_venda) = 2024 
                AND v.data_venda >= '2024-01-01' 
                AND EXTRACT(MONTH FROM v.data_venda) < p.ultimo_mes)
            )
        GROUP BY EXTRACT(YEAR FROM v.data_venda)
    )
    SELECT 
        MAX(CASE WHEN ano = 2024 THEN faturamento_total END) as faturamento_2024,
        MAX(CASE WHEN ano = 2025 THEN faturamento_total END) as faturamento_2025,
        MAX(CASE WHEN ano = 2025 THEN faturamento_total END) - 
        MAX(CASE WHEN ano = 2024 THEN faturamento_total END) as diferenca,
        ROUND(
            ((MAX(CASE WHEN ano = 2025 THEN faturamento_total END) - 
            MAX(CASE WHEN ano = 2024 THEN faturamento_total END)) / 
            NULLIF(MAX(CASE WHEN ano = 2024 THEN faturamento_total END), 0)) * 100, 
            2
        ) as variacao_percentual
    FROM faturamento_comparativo
"""
df_faturamento = executar_query(query6)
print("Faturamento 2024 (até o último mês fechado): R$", df_faturamento['faturamento_2024'][0])
print("Faturamento 2025 (até o último mês fechado): R$", df_faturamento['faturamento_2025'][0])
print("Diferença de faturamento: R$", df_faturamento['diferenca'][0])
print("Variação percentual: ", df_faturamento['variacao_percentual'][0], "%")

####
# Elabore uma tabela com a venda por grupo de mercadoria mostrando o crescimento ou decréscimo de vendas nos três últimos meses
####
print("\n")
print("\n")
print("#### PERGUNTA 7 ####")
query7 = """
    WITH meses AS (
        SELECT 
            DATE_TRUNC('month', '2025-10-01'::DATE) AS mes_atual,
            DATE_TRUNC('month', '2025-10-01'::DATE) - INTERVAL '1 month' AS mes_anterior,
            DATE_TRUNC('month', '2025-10-01'::DATE) - INTERVAL '2 months' AS mes_anterior_2
    ),
    vendas_mensais AS (
        SELECT 
            c.nome_categoria AS grupo_mercadoria,
            DATE_TRUNC('month', v.data_venda) AS mes,
            SUM(vi.total_item) AS total_vendas
        FROM maloka_core.venda v
        JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
        JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
        JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
        WHERE v.situacao_venda = 'CONCLUIDA'
            AND v.tipo_venda = 'PEDIDO'
            AND DATE_TRUNC('month', v.data_venda) IN (
                (SELECT mes_anterior_2 FROM meses),
                (SELECT mes_anterior FROM meses),
                (SELECT mes_atual FROM meses)
            )
            AND p.ativo = true
            AND p.excluido = false
        GROUP BY c.nome_categoria, DATE_TRUNC('month', v.data_venda)
    ),
    vendas_agrupadas AS (
        SELECT 
            grupo_mercadoria,
            SUM(CASE WHEN mes = (SELECT mes_anterior_2 FROM meses) THEN total_vendas ELSE 0 END) AS vendas_mes_menos_2,
            SUM(CASE WHEN mes = (SELECT mes_anterior FROM meses) THEN total_vendas ELSE 0 END) AS vendas_mes_menos_1,
            SUM(CASE WHEN mes = (SELECT mes_atual FROM meses) THEN total_vendas ELSE 0 END) AS vendas_mes_atual
        FROM vendas_mensais
        GROUP BY grupo_mercadoria
    ),
    vendas_com_variacao AS (
        SELECT 
            grupo_mercadoria,
            vendas_mes_menos_2,
            vendas_mes_menos_1,
            vendas_mes_atual,
            CASE 
                WHEN vendas_mes_menos_2 = 0 THEN NULL
                ELSE ROUND(((vendas_mes_menos_1 - vendas_mes_menos_2) / vendas_mes_menos_2) * 100, 2)
            END AS crescimento_mes_menos_2_para_menos_1,
            CASE 
                WHEN vendas_mes_menos_1 = 0 THEN NULL
                ELSE ROUND(((vendas_mes_atual - vendas_mes_menos_1) / vendas_mes_menos_1) * 100, 2)
            END AS crescimento_mes_menos_1_para_atual
        FROM vendas_agrupadas
    )
    SELECT 
        grupo_mercadoria,
        vendas_mes_menos_2,
        vendas_mes_menos_1,
        vendas_mes_atual,
        crescimento_mes_menos_2_para_menos_1,
        crescimento_mes_menos_1_para_atual
    FROM vendas_com_variacao
    WHERE vendas_mes_menos_2 > 0 OR vendas_mes_menos_1 > 0 OR vendas_mes_atual > 0
    ORDER BY grupo_mercadoria;
"""
df_crescimento_mercadorias = executar_query(query7)
# mostrar 10 primeiros da tabela
print("Crescimento de vendas por grupo de mercadoria nos últimos 3 meses:")
print(df_crescimento_mercadorias.head(10))

####
# Quais clientes compraram o produto "xxxxxxx" no último mês
####
print("\n")
print("\n")
print("#### PERGUNTA 8 ####")
query8 = f"""
    SELECT DISTINCT
        c.id_cliente,
        c.nome,
        c.email,
        c.telefone,
        COUNT(DISTINCT v.id_venda) as quantidade_compras
    FROM maloka_core.venda v
    INNER JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
    INNER JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
    INNER JOIN maloka_core.cliente c ON v.id_cliente = c.id_cliente
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND p.nome = '{general_config["produtoX"]}'
        AND v.data_venda >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
        AND v.data_venda < DATE_TRUNC('month', NOW())
    GROUP BY c.id_cliente, c.nome, c.email, c.telefone
    ORDER BY quantidade_compras DESC
"""
df_clientes = executar_query(query8)
print(f"Clientes que compraram o produto {general_config["produtoX"]} no último mês:")
print(df_clientes)

#####
# Quais as vendas este ano do produto "yyyyyyyy" até o dia de ontem
#####
print("\n")
print("\n")
print("#### PERGUNTA 9 ####")
query9 = f"""
    SELECT
        v.id_venda,
        v.data_venda,
        c.nome as cliente,
        vi.quantidade,
        vi.preco_bruto,
        vi.desconto,
        vi.total_item,
        l.nome as loja
    FROM maloka_core.venda v
    INNER JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
    INNER JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
    LEFT JOIN maloka_core.cliente c ON v.id_cliente = c.id_cliente
    LEFT JOIN maloka_core.loja l ON v.id_loja = l.id_loja
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND p.nome = '{general_config["produtoY"]}'
        AND v.data_venda >= '2025-01-01'
        AND v.data_venda < CURRENT_DATE
    ORDER BY v.data_venda DESC
"""
df_vendas_produto = executar_query(query9)
print("Vendas do produto", general_config["produtoY"], "em 2025 até ontem:")
print(df_vendas_produto)

#####
# Qual o ticket médio dos clientes campeões
#####
print("\n")
print("\n")
print("#### PERGUNTA 10 ####")
query10 = """
    SELECT
        ROUND(SUM(v.total_venda) / COUNT(DISTINCT v.id_venda), 2) as ticket_medio_campeoes,
        COUNT(DISTINCT v.id_venda) as quantidade_vendas_campeoes,
        SUM(v.total_venda) as valor_total_vendas_campeoes,
        COUNT(DISTINCT v.id_cliente) as quantidade_clientes_campeoes
    FROM maloka_core.venda v
    INNER JOIN maloka_analytics.segmentacao s ON v.id_cliente = s.id_cliente
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND s.segmento = 'Campeões'
"""
df_ticket_medio = executar_query(query10)
print("Ticket médio dos clientes campeões: R$", df_ticket_medio['ticket_medio_campeoes'][0])
print("Quantidade de vendas para campeões:", df_ticket_medio['quantidade_vendas_campeoes'][0])
print("Valor total de vendas para campeões: R$", df_ticket_medio['valor_total_vendas_campeoes'][0])
print("Quantidade de clientes campeões com vendas:", df_ticket_medio['quantidade_clientes_campeoes'][0])

####
# Qual o ticket médio geral de 2025
####
print("\n")
print("\n")
print("#### PERGUNTA 11 ####")
query11 = """
    SELECT
        ROUND(SUM(v.total_venda) / COUNT(v.id_venda), 2) as ticket_medio_2025
    FROM maloka_core.venda v
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= '2025-01-01'
        AND v.data_venda < '2026-01-01'
"""
df_ticket_medio_2025 = executar_query(query11)
print("Ticket médio geral de 2025: R$", df_ticket_medio_2025['ticket_medio_2025'][0])

####
# Compare o ticket médio dos clientes fiéis com o ticket médio geral
####
print("\n")
print("\n")
print("#### PERGUNTA 12 ####")
query12 = """
    WITH ticket_fieis AS (
        SELECT
            ROUND(SUM(v.total_venda) / COUNT(v.id_venda), 2) as ticket_medio
        FROM maloka_core.venda v
        INNER JOIN maloka_analytics.segmentacao s ON v.id_cliente = s.id_cliente
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND s.segmento LIKE 'Fiéis%'
    ),
    ticket_geral AS (
        SELECT
            ROUND(SUM(v.total_venda) / COUNT(v.id_venda), 2) as ticket_medio
        FROM maloka_core.venda v
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
    )
    SELECT
        tf.ticket_medio as ticket_medio_fieis,
        tg.ticket_medio as ticket_medio_geral,
        ROUND(tf.ticket_medio - tg.ticket_medio, 2) as diferenca,
        ROUND(((tf.ticket_medio - tg.ticket_medio) / NULLIF(tg.ticket_medio, 0)) * 100, 2) as variacao_percentual
    FROM ticket_fieis tf
    CROSS JOIN ticket_geral tg
"""
df_comparacao_tickets = executar_query(query12)
print("Comparação de tickets:")
print(df_comparacao_tickets)

####
# Calcule o valor médio de produtos vendidos para os clientes campeões
####
print("\n")
print("\n")
print("#### PERGUNTA 13 ####")
query13 = """
    WITH vendas_campeoes as (
        SELECT id_venda, v.id_cliente, segmento
        FROM maloka_core.venda v
        JOIN maloka_core.segmentacao seg on v.id_cliente = seg.id_cliente
        where segmento = 'CampeÃµes' and v.tipo_venda = 'PEDIDO' and v.situacao_venda = 'CONCLUIDA'
    ),
    detalhes AS (
        SELECT 
            vc.id_venda,
            vc.id_cliente,
            vi.id_produto,
            p.nome,
            vi.quantidade,
            vi.total_item
        FROM vendas_campeoes vc 
        JOIN maloka_core.venda_item vi ON vc.id_venda = vi.id_venda
        JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
    )
    SELECT 
        SUM(total_item) AS soma_total_item,
        SUM(quantidade) AS soma_quantidade,
        ROUND(SUM(total_item) / NULLIF(SUM(quantidade),0), 2) AS preco_medio_geral
    FROM detalhes;
"""
df_valor_medio_produtos = executar_query(query13)
print("Valor médio de produtos vendidos para os clientes campeões: R$", df_valor_medio_produtos['preco_medio_geral'][0])

####
# Qual o percentual de vendas no último mês para os clientes campeões por categoria de produtos
####
print("\n")
print("\n")
print("#### PERGUNTA 14 ####")
query14 = """
    WITH total_vendas AS (
        SELECT
            SUM(v.total_venda) as total
        FROM maloka_core.venda v
        INNER JOIN maloka_analytics.segmentacao s ON v.id_cliente = s.id_cliente
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND s.segmento = 'Campeões'
            AND v.data_venda >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
            AND v.data_venda < DATE_TRUNC('month', NOW())
    ),
    vendas_categoria AS (
        SELECT
            c.nome_categoria,
            SUM(vi.total_item) as total_categoria
        FROM maloka_core.venda v
        INNER JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
        INNER JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
        LEFT JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
        INNER JOIN maloka_analytics.segmentacao s ON v.id_cliente = s.id_cliente
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND vi.tipo = 'P'
            AND s.segmento = 'Campeões'
            AND v.data_venda >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
            AND v.data_venda < DATE_TRUNC('month', NOW())
        GROUP BY c.nome_categoria
    )
    SELECT
        vc.nome_categoria,
        vc.total_categoria,
        ROUND((vc.total_categoria / NULLIF(tv.total, 0)) * 100, 2) as percentual
    FROM vendas_categoria vc
    CROSS JOIN total_vendas tv
    ORDER BY percentual DESC
"""
df_percentual_vendas_categoria = executar_query(query14)
print("Percentual de vendas no último mês para os clientes campeões por categoria de produtos:")
print(df_percentual_vendas_categoria)

####
# Gere um ranking de total de vendas da última semana por vendedor
####
print("\n")
print("\n")
print("#### PERGUNTA 15 ####")
query15 = """
    SELECT 
        vd.id_vendedor,
        vd.nome,
        sum(v.total_venda) AS total_vendas
    FROM maloka_core.vendedor vd
    JOIN maloka_core.venda v 
        ON v.id_vendedor = vd.id_vendedor
    WHERE v.data_venda >= DATE '2025-09-24'
        AND v.data_venda <= DATE '2025-09-30'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.tipo_venda = 'PEDIDO'
    GROUP BY vd.id_vendedor, vd.nome
    ORDER BY vd.id_vendedor ASC;
"""

df_ranking_vendas = executar_query(query15)
# Mostrar top 10 do ranking
print("Ranking de total de vendas da última semana por vendedor:")
print(df_ranking_vendas.head(10))

####
# Gere as vendas de um vendedor "zzzzzz" por categoria de material
####
print("\n")
print("\n")
print("#### PERGUNTA 16 ####")
query16 = f"""
    SELECT 
        c.nome_categoria,
        SUM(vi.total_item) AS total_vendido,
        SUM(vi.quantidade) AS total_itens
    FROM maloka_core.venda v
    JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
    JOIN maloka_core.produto p 
        ON vi.id_produto = p.id_produto
    JOIN maloka_core.categoria c 
        ON p.id_categoria = c.id_categoria
    JOIN maloka_core.vendedor vd
        ON v.id_vendedor = vd.id_vendedor
    WHERE v.tipo_venda = 'PEDIDO'
    AND v.situacao_venda = 'CONCLUIDA'
    AND vd.nome = '{general_config["vendedor"]}'
    AND p.ativo = true
    AND p.excluido = false
    GROUP BY c.nome_categoria
    ORDER BY total_vendido DESC;
"""
df_vendas_vendedor = executar_query(query16)
print(f"Vendas do vendedor {general_config["vendedor"]} por categoria de material:")
print(df_vendas_vendedor)

####
# Liste em uma tabela o ticket médio de vendas por vendedor no mês passado
####
print("\n")
print("\n")
print("#### PERGUNTA 17 ####")
query17 = """
    SELECT
        vd.id_vendedor,
        vd.nome as vendedor,
        COUNT(v.id_venda) as quantidade_vendas,
        SUM(v.total_venda) as total_vendas,
        ROUND(SUM(v.total_venda) / COUNT(v.id_venda), 2) as ticket_medio
    FROM maloka_core.venda v
    INNER JOIN maloka_core.vendedor vd ON v.id_vendedor = vd.id_vendedor
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
        AND v.data_venda < DATE_TRUNC('month', NOW())
    GROUP BY vd.id_vendedor, vd.nome
    ORDER BY ticket_medio DESC
"""
df_ticket_medio_vendedor = executar_query(query17)
print("Ticket médio de vendas por vendedor no mês passado:")
print(df_ticket_medio_vendedor)

####
# Gere as vendas de ontem por loja
####
print("\n")
print("\n")
print("#### PERGUNTA 18 ####")
query18 = """
    SELECT
        l.id_loja,
        l.nome as loja,
        COUNT(v.id_venda) as quantidade_vendas,
        SUM(v.total_venda) as total_vendas
    FROM maloka_core.venda v
    INNER JOIN maloka_core.loja l ON v.id_loja = l.id_loja
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= CURRENT_DATE - INTERVAL '1 day'
        AND v.data_venda < CURRENT_DATE
    GROUP BY l.id_loja, l.nome
    ORDER BY total_vendas DESC
"""
df_vendas_ontem_loja = executar_query(query18)
print("Vendas de ontem por loja:")
print(df_vendas_ontem_loja)

####
# Gere o ticket médio de vendas no mês passado por loja
####
print("\n")
print("\n")
print("#### PERGUNTA 19 ####")
query19 = """
    SELECT
        l.id_loja,
        l.nome as loja,
        COUNT(v.id_venda) as quantidade_vendas,
        SUM(v.total_venda) as total_vendas,
        ROUND(SUM(v.total_venda) / COUNT(v.id_venda), 2) as ticket_medio
    FROM maloka_core.venda v
    INNER JOIN maloka_core.loja l ON v.id_loja = l.id_loja
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
        AND v.data_venda < DATE_TRUNC('month', NOW())
    GROUP BY l.id_loja, l.nome
    ORDER BY ticket_medio DESC
"""
df_ticket_medio_loja = executar_query(query19)
print("Ticket médio de vendas no mês passado por loja:")
print(df_ticket_medio_loja) 

####
# Liste um ranking com o total de vendas de ontem, por loja
####
print("\n")
print("\n")
print("#### PERGUNTA 20 ####")
query20 = """
    SELECT
        l.id_loja,
        l.nome as loja,
        COUNT(v.id_venda) as quantidade_vendas,
        SUM(v.total_venda) as total_vendas
    FROM maloka_core.venda v
    INNER JOIN maloka_core.loja l ON v.id_loja = l.id_loja
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= CURRENT_DATE - INTERVAL '1 day'
        AND v.data_venda < CURRENT_DATE
    GROUP BY l.id_loja, l.nome
    ORDER BY total_vendas DESC
"""
df_vendas_ontem_loja = executar_query(query20)
print("Total de vendas de ontem por loja:")
print(df_vendas_ontem_loja)

####
# Liste 5 produtos da categoria "xxxxx"
####
print("\n")
print("\n")
print("#### PERGUNTA 21 ####")
query21 = f"""
    SELECT
        p.id_produto,
        p.nome as produto,
        p.preco_venda,
        p.preco_custo,
        p.codigo_barras,
        m.nome as marca
    FROM maloka_core.produto p
    LEFT JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
    LEFT JOIN maloka_core.marca m ON p.id_marca = m.id_marca
    WHERE c.nome_categoria = '{general_config["categoria"]}'
        AND p.ativo = true
        AND p.excluido = false
    LIMIT 5
"""
df_produtos_categoria = executar_query(query21)
print(f"Produtos da categoria {general_config["categoria"]}:")
print(df_produtos_categoria)

####
# Liste 10 clientes campeões
####
print("\n")
print("\n")
print("#### PERGUNTA 22 ####")
query22 = """
    SELECT
        c.id_cliente,
        c.nome,
        c.email,
        c.telefone,
        c.cidade,
        c.estado,
        s.valor_monetario,
        s.frequencia,
        s.recencia
    FROM maloka_core.cliente c
    INNER JOIN maloka_analytics.segmentacao s ON c.id_cliente = s.id_cliente
    WHERE s.segmento = 'Campeões'
    ORDER BY s.valor_monetario DESC
    LIMIT 10
"""
df_clientes_campeoes = executar_query(query22)
print("Clientes campeões:")
print(df_clientes_campeoes)

####
# Liste 5 clientes sumidos
####
print("\n")
print("\n")
print("#### PERGUNTA 23 ####")
query23 = """
    SELECT
        c.id_cliente,
        c.nome,
        c.email,
        c.telefone,
        c.cidade,
        c.estado,
        s.recencia,
        s.valor_monetario,
        s.antiguidade
    FROM maloka_core.cliente c
    INNER JOIN maloka_analytics.segmentacao s ON c.id_cliente = s.id_cliente
    WHERE s.segmento = 'Sumidos'
    ORDER BY s.recencia DESC
    LIMIT 5
"""
df_clientes_sumidos = executar_query(query23)
print("Clientes sumidos:")
print(df_clientes_sumidos)

####
# Mostre os 10 produtos campeões em vendas na última semana
####
print("\n")
print("\n")
print("#### PERGUNTA 24 ####")
query24 = """
    WITH ultima_semana AS (
        SELECT 
            CURRENT_DATE - INTERVAL '7 days' AS data_inicio,
            CURRENT_DATE AS data_fim
    ),
    vendas_ultima_semana AS (
        SELECT 
            vi.id_produto,
            SUM(vi.quantidade) AS quantidade_vendida,
            SUM(vi.total_item) AS valor_total_vendido
        FROM maloka_core.venda_item vi
        INNER JOIN maloka_core.venda v ON vi.id_venda = v.id_venda
        CROSS JOIN ultima_semana us
        WHERE v.data_venda >= us.data_inicio 
            AND v.data_venda < us.data_fim
            AND v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND vi.id_produto IS NOT NULL
        GROUP BY vi.id_produto
    )
    SELECT 
        p.nome AS nome_produto,
        c.nome_categoria,
        v.quantidade_vendida,
        v.valor_total_vendido
    FROM vendas_ultima_semana v
    INNER JOIN maloka_core.produto p ON v.id_produto = p.id_produto
    LEFT JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
    WHERE p.ativo = true AND p.excluido = false
    ORDER BY v.valor_total_vendido DESC
    LIMIT 10;
"""
df_campeoes_vendas = executar_query(query24)
print("Produtos campeões em vendas na última semana:")
print(df_campeoes_vendas)

####
# Mostre o vendedor campeão em vendas no mês passado
####
print("\n")
print("\n")
print("#### PERGUNTA 25 ####")
query25 = """
    SELECT
        vd.id_vendedor,
        vd.nome as vendedor,
        COUNT(v.id_venda) as quantidade_vendas,
        SUM(v.total_venda) as total_vendas
    FROM maloka_core.venda v
    INNER JOIN maloka_core.vendedor vd ON v.id_vendedor = vd.id_vendedor
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
        AND v.data_venda < DATE_TRUNC('month', NOW())
    GROUP BY vd.id_vendedor, vd.nome
    ORDER BY total_vendas DESC
    LIMIT 1
"""

df_vendedor_campeao = executar_query(query25)
print("Vendedor campeão em vendas no mês passado:")
print(df_vendedor_campeao)

####
#Analise as vendas do último trimestre comparado ao mesmo período no ano anterior
####
print("\n")
print("\n")
print("#### PERGUNTA 26 ####")
query26 = """
    WITH trimestre_atual AS (
        SELECT
            'Trimestre Atual' as periodo,
            SUM(v.total_venda) as total_vendas,
            COUNT(v.id_venda) as quantidade_vendas
        FROM maloka_core.venda v
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND v.data_venda >= DATE_TRUNC('quarter', NOW() - INTERVAL '3 months')
            AND v.data_venda < DATE_TRUNC('quarter', NOW())
    ),
    trimestre_anterior AS (
        SELECT
            'Trimestre Ano Anterior' as periodo,
            SUM(v.total_venda) as total_vendas,
            COUNT(v.id_venda) as quantidade_vendas
        FROM maloka_core.venda v
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND v.data_venda >= DATE_TRUNC('quarter', NOW() - INTERVAL '3 months') - INTERVAL '1 year'
            AND v.data_venda < DATE_TRUNC('quarter', NOW()) - INTERVAL '1 year'
    )
    SELECT
        ta.periodo,
        ta.total_vendas as total_vendas_atual,
        tap.total_vendas as total_vendas_anterior,
        ROUND(ta.total_vendas - tap.total_vendas, 2) as diferenca,
        ROUND(((ta.total_vendas - tap.total_vendas) / NULLIF(tap.total_vendas, 0)) * 100, 2) as variacao_percentual,
        ta.quantidade_vendas as qtd_vendas_atual,
        tap.quantidade_vendas as qtd_vendas_anterior
    FROM trimestre_atual ta
    CROSS JOIN trimestre_anterior tap
"""

df_comparativo_trimestre = executar_query(query26)
print("Comparativo de vendas do último trimestre:")
print(df_comparativo_trimestre)

####
# Qual a venda de serviço este mês?
####
print("\n")
print("\n")
print("#### PERGUNTA 27 ####")
query27 = """
    SELECT
        COUNT(DISTINCT v.id_venda) as quantidade_vendas,
        SUM(vi.total_item) as total_vendas_servico
    FROM maloka_core.venda v
    INNER JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND vi.tipo = 'S'
        AND v.data_venda >= DATE_TRUNC('month', NOW())
        AND v.data_venda < DATE_TRUNC('month', NOW()) + INTERVAL '1 month'
"""
df_venda_servico_mes = executar_query(query27)
print("Venda de serviço este mês:")
print(df_venda_servico_mes)

####
# Qual o ticket médio de serviço?
####
print("\n")
print("\n")
print("#### PERGUNTA 28 ####")
query28 = """
    WITH vendas_com_servico AS (
        SELECT
            v.id_venda,
            SUM(vi.total_item) as total_servico
        FROM maloka_core.venda v
        INNER JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
        WHERE v.tipo_venda = 'PEDIDO'
            AND v.situacao_venda = 'CONCLUIDA'
            AND vi.tipo = 'S'
        GROUP BY v.id_venda
    )
    SELECT
        ROUND(AVG(vcs.total_servico), 2) as ticket_medio_servico
    FROM vendas_com_servico vcs
"""
df_ticket_medio_servico = executar_query(query28)
print("Ticket médio de serviço:")
print(df_ticket_medio_servico)

####
# Liste o ranking de vendas por loja e por vendedor em agosto de 2025
####
print("\n")
print("\n")
print("#### PERGUNTA 29 ####")
query29 = """
    SELECT
        l.nome as loja,
        vd.nome as vendedor,
        COUNT(v.id_venda) as quantidade_vendas,
        SUM(v.total_venda) as total_vendas
    FROM maloka_core.venda v
    INNER JOIN maloka_core.loja l ON v.id_loja = l.id_loja
    INNER JOIN maloka_core.vendedor vd ON v.id_vendedor = vd.id_vendedor
    WHERE v.tipo_venda = 'PEDIDO'
        AND v.situacao_venda = 'CONCLUIDA'
        AND v.data_venda >= '2025-08-01'
        AND v.data_venda < '2025-09-01'
    GROUP BY l.nome, vd.nome
    ORDER BY total_vendas DESC
"""
df_ranking_vendas = executar_query(query29)
print("Ranking de vendas por loja e por vendedor em agosto de 2025:")
print(df_ranking_vendas)

####
# Analise as vendas da categoria de produtos "xxxx" no último trimestre e indique ações para melhorar as vendas
####
print("\n")
print("\n")
print("#### PERGUNTA 30 ####")
query30 = f"""
    WITH vendas_trimestre_atual AS (
        SELECT 
            vi.id_produto,
            p.nome AS nome_produto,
            SUM(vi.quantidade) AS quantidade_vendida,
            SUM(vi.total_item) AS valor_vendido,
            COUNT(DISTINCT v.id_venda) AS num_vendas
        FROM maloka_core.venda_item vi
        JOIN maloka_core.venda v ON vi.id_venda = v.id_venda
        JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
        JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
        WHERE v.data_venda >= '2025-07-01' 
            AND v.data_venda < '2025-10-01'
            AND v.situacao_venda = 'CONCLUIDA'
            AND v.tipo_venda = 'PEDIDO'
            AND c.nome_categoria = '{general_config["categoria"]}'
            AND p.ativo = true 
            AND p.excluido = false
        GROUP BY vi.id_produto, p.nome
    ),
    vendas_trimestre_anterior AS (
        SELECT 
            vi.id_produto,
            SUM(vi.quantidade) AS quantidade_vendida_anterior,
            SUM(vi.total_item) AS valor_vendido_anterior
        FROM maloka_core.venda_item vi
        JOIN maloka_core.venda v ON vi.id_venda = v.id_venda
        JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
        JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
        WHERE v.data_venda >= '2024-07-01' 
            AND v.data_venda < '2024-10-01'
            AND v.situacao_venda = 'CONCLUIDA'
            AND v.tipo_venda = 'PEDIDO'
            AND c.nome_categoria = '{general_config["categoria"]}'
            AND p.ativo = true 
            AND p.excluido = false
        GROUP BY vi.id_produto
    ),
    estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        JOIN maloka_core.produto p ON he.id_produto = p.id_produto
        JOIN maloka_core.categoria c ON p.id_categoria = c.id_categoria
        WHERE c.nome_categoria = '{general_config["categoria"]}'
            AND p.ativo = true 
            AND p.excluido = false
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    ),
    estoque_total_produto AS (
        SELECT
            ea.id_produto,
            SUM(ea.estoque) as estoque_total
        FROM estoque_atual ea
        GROUP BY ea.id_produto
    ),
    top_clientes AS (
        SELECT 
            v.id_cliente,
            c.nome AS nome_cliente,
            c.tipo AS tipo_cliente,
            SUM(vi.total_item) AS valor_total_comprado,
            COUNT(DISTINCT v.id_venda) AS num_compras
        FROM maloka_core.venda v
        JOIN maloka_core.venda_item vi ON v.id_venda = vi.id_venda
        JOIN maloka_core.produto p ON vi.id_produto = p.id_produto
        JOIN maloka_core.categoria cat ON p.id_categoria = cat.id_categoria
        JOIN maloka_core.cliente c ON v.id_cliente = c.id_cliente
        WHERE v.data_venda >= '2025-07-01' 
            AND v.data_venda < '2025-10-01'
            AND v.situacao_venda = 'CONCLUIDA'
            AND v.tipo_venda = 'PEDIDO'
            AND cat.nome_categoria = '{general_config["categoria"]}'
            AND p.ativo = true 
            AND p.excluido = false
        GROUP BY v.id_cliente, c.nome, c.tipo
        ORDER BY valor_total_comprado DESC
        LIMIT 10
    )
    SELECT 
        vta.id_produto,
        vta.nome_produto,
        vta.quantidade_vendida,
        vta.valor_vendido,
        vta.num_vendas,
        COALESCE(vta_anterior.quantidade_vendida_anterior, 0) AS quantidade_vendida_anterior,
        COALESCE(vta_anterior.valor_vendido_anterior, 0) AS valor_vendido_anterior,
        CASE 
            WHEN COALESCE(vta_anterior.quantidade_vendida_anterior, 0) = 0 THEN 100.0
            ELSE ((vta.quantidade_vendida - vta_anterior.quantidade_vendida_anterior) / vta_anterior.quantidade_vendida_anterior) * 100
        END AS variacao_percentual_quantidade,
        etp.estoque_total,
        CASE 
            WHEN etp.estoque_total <= 0 THEN 'ESTOQUE ZERADO'
            WHEN etp.estoque_total < 10 THEN 'ESTOQUE BAIXO'
            WHEN etp.estoque_total > 100 THEN 'ESTOQUE ALTO'
            ELSE 'ESTOQUE ADEQUADO'
        END AS situacao_estoque,
        RANK() OVER (ORDER BY vta.valor_vendido DESC) AS ranking_vendas
    FROM vendas_trimestre_atual vta
    LEFT JOIN vendas_trimestre_anterior vta_anterior ON vta.id_produto = vta_anterior.id_produto
    LEFT JOIN estoque_total_produto etp ON vta.id_produto = etp.id_produto
    ORDER BY vta.valor_vendido DESC;
"""
df_analise_categoria = executar_query(query30)
print(f"Análise da categoria {general_config['categoria']} no último trimestre:")
print(df_analise_categoria.head(10))  # Mostrar apenas os 10 primeiros

####
# Quais produtos estão com estoque zerado?
####
print("\n")
print("\n")
print("#### PERGUNTA 31 ####")
query31 = """
    WITH estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    ),
    estoque_total_produto AS (
        SELECT
            ea.id_produto,
            SUM(ea.estoque) as estoque_total
        FROM estoque_atual ea
        GROUP BY ea.id_produto
    )
    select * 
    from estoque_total_produto etp
    join maloka_core.produto p on etp.id_produto = p.id_produto
    where estoque_total <= 0 and p.ativo = true and p.excluido = false
"""
df_produtos_zerados = executar_query(query31)
# total
print("Total de produtos com estoque zerado:", df_produtos_zerados.shape[0])
print("Produtos com estoque zerado:")
print(df_produtos_zerados.head(15))  # Mostrar apenas os 15 primeiros

####
# Liste os 20 produtos com maior quantidade em estoque
####
print("\n")
print("\n")
print("#### PERGUNTA 32 ####")
query32 = """
    WITH estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    ),
    estoque_total_produto AS (
        SELECT
            ea.id_produto,
            SUM(ea.estoque) as estoque_total
        FROM estoque_atual ea
        GROUP BY ea.id_produto
    )
    select p.id_produto, p.nome, estoque_total
    from estoque_total_produto etp
    join maloka_core.produto p on etp.id_produto = p.id_produto
    where p.ativo = true and p.excluido = false
    order by estoque_total desc
    limit 20
"""
df_top_estoque = executar_query(query32)
print("Top 20 produtos com maior quantidade em estoque:")
print(df_top_estoque.head(20))  # Mostrar apenas os 20 primeiros

####
# Quais produtos não tiveram movimentação de estoque nos últimos 30 dias?
####
print("\n")
print("\n")
print("#### PERGUNTA 34 ####")
query34 = """
    WITH mov_30 AS (
        SELECT DISTINCT id_produto
        FROM maloka_core.estoque_movimento
        WHERE data_movimento >= '2025-09-01' 
            AND data_movimento < '2025-10-01'
    )
    SELECT p.id_produto
    FROM maloka_core.produto p
    WHERE p.ativo = true and p.excluido = false and p.id_produto NOT IN (SELECT id_produto FROM mov_30);
"""
df_estoque_sem_movimentacao = executar_query(query34)
print("Total de produtos sem movimentação de estoque nos últimos 30 dias:", df_estoque_sem_movimentacao.shape[0])
print("Produtos sem movimentação de estoque nos últimos 30 dias:")
print(df_estoque_sem_movimentacao.head(15))  # Mostrar apenas os 15 primeiros

####
# Mostre a movimentação de estoque de ontem por tipo (entrada/saída)
####
print("\n")
print("\n")
print("#### PERGUNTA 36 ####")
query36 = """
    SELECT 
        em.tipo,
        SUM(em.quantidade) AS quantidade_total
    FROM maloka_core.estoque_movimento em
    WHERE em.data_movimento >= '2025-10-01' AND em.data_movimento < '2025-10-02'
    GROUP BY em.tipo
    ORDER BY quantidade_total DESC;
"""
df_movimentacao_ontem = executar_query(query36)
print("Total de tipos de movimentação de estoque de ontem:", df_movimentacao_ontem.shape[0])
print("Movimentação de estoque de ontem por tipo:")
print(df_movimentacao_ontem)

####
# Quais foram as últimas 10 entradas de estoque por compra?
####
print("\n")
print("\n")
print("#### PERGUNTA 37 ####")
query37 = """
    SELECT p.nome, em.data_movimento, em.id_compra
    FROM maloka_core.estoque_movimento em
    JOIN maloka_core.produto p on p.id_produto = em.id_produto
    where em.tipo = 'E' and em.origem_movimento = 'C' and p.ativo = true 
    and p.excluido = false 
    ORDER BY em.data_movimento desc
    limit 10
"""
df_ultimas_entradas = executar_query(query37)
print("Últimas 10 entradas de estoque por compra:")
print(df_ultimas_entradas)

####
# Quais produtos tiveram mais entradas de estoque no último trimestre?
####
print("\n")
print("\n")
print("#### PERGUNTA 40 ####")
query40 = """
    SELECT 
        p.nome,
        em.id_produto,
        SUM(em.quantidade) AS total_quantidade
    FROM maloka_core.estoque_movimento em
    JOIN maloka_core.produto p ON p.id_produto = em.id_produto
    WHERE em.tipo = 'E'
    AND p.ativo = TRUE
    AND em.data_movimento >= '2025-07-01'
    AND em.data_movimento <= '2025-09-30'
    AND p.excluido = FALSE
    GROUP BY p.nome, em.id_produto
    ORDER BY total_quantidade DESC;
"""
df_produtos_mais_entradas = executar_query(query40)
print("Produtos com mais entradas de estoque no último trimestre:")
print(df_produtos_mais_entradas.head(15))  # Mostrar apenas os 15 primeiros

####
# Compare as entradas vs saídas de estoque do mês passado
####
print("\n")
print("\n")
print("#### PERGUNTA 41 ####")
query41 = """
    SELECT
        COALESCE(SUM(em.quantidade) FILTER (WHERE em.tipo = 'E'), 0) AS total_entradas,
        COALESCE(SUM(em.quantidade) FILTER (WHERE em.tipo = 'S'), 0) AS total_saidas,
        COALESCE(SUM(em.quantidade) FILTER (WHERE em.tipo = 'E'), 0)
        - COALESCE(SUM(em.quantidade) FILTER (WHERE em.tipo = 'S'), 0) AS saldo_liquido
    FROM maloka_core.estoque_movimento em
    WHERE em.data_movimento >= '2025-09-01'
    AND em.data_movimento <= '2025-09-30';
"""
df_entradas_vs_saidas = executar_query(query41)
print("Entradas vs Saídas de Estoque do Mês Passado:")
print(df_entradas_vs_saidas)

####
# Qual o valor total do estoque atual (baseado no preço de custo)?
####
print("\n")
print("\n")
print("#### PERGUNTA 42 ####")
query42 = """
    WITH estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    )
    SELECT
        SUM(ea.estoque * p.preco_custo) AS valor_total
    FROM estoque_atual ea
    JOIN maloka_core.produto p on p.id_produto = ea.id_produto
    where p.ativo = true and p.excluido = false
"""
df_valor_estoque = executar_query(query42)
print("Valor total do estoque atual (baseado no preço de custo):")
print(df_valor_estoque)

####
# Qual o valor total do estoque atual (baseado no preço de venda)?
####
print("\n")
print("\n")
print("#### PERGUNTA 43 ####")
query43 = """
    WITH estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    )
    SELECT
        SUM(ea.estoque * p.preco_venda) AS valor_total
    FROM estoque_atual ea
    JOIN maloka_core.produto p on p.id_produto = ea.id_produto
    where p.ativo = true and p.excluido = false
"""
df_valor_estoque_venda = executar_query(query43)
print("Valor total do estoque atual (baseado no preço de venda):")
print(df_valor_estoque_venda)

####
#  Liste os 10 produtos que representam maior valor em estoque
####
print("\n")
print("\n")
print("#### PERGUNTA 44 ####")
query44 = """
    WITH estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    )
    SELECT
        ea.id_produto,
        SUM(ea.estoque) AS estoque_total,
        p.preco_venda,
        SUM(ea.estoque * p.preco_venda) AS valor_total_produto
    FROM estoque_atual ea
    JOIN maloka_core.produto p ON p.id_produto = ea.id_produto
    WHERE p.ativo = TRUE
    AND p.excluido = FALSE
    GROUP BY ea.id_produto, p.preco_venda
    order by valor_total_produto desc
    limit 10
"""
df_produtos_maior_valor_estoque = executar_query(query44)
print("Top 10 produtos que representam maior valor em estoque:")
print(df_produtos_maior_valor_estoque)

####
# Mostre produtos com estoque parado há mais de 90 dias e seu valor total
####
print("\n")
print("\n")
print("#### PERGUNTA 45 ####")
query45 = """
    WITH produtos_ativos AS (
        SELECT 
            p.id_produto,
            p.nome,
            p.codigo_barras
        FROM maloka_core.produto p
        WHERE p.ativo = true AND p.excluido = false
    ),
    ultima_venda_produto AS (
        SELECT 
            vi.id_produto,
            MAX(v.data_venda) AS data_ultima_venda
        FROM maloka_core.venda_item vi
        INNER JOIN maloka_core.venda v ON vi.id_venda = v.id_venda
        WHERE v.tipo_venda = 'PEDIDO' AND v.situacao_venda = 'CONCLUIDA'
        GROUP BY vi.id_produto
    )
    SELECT 
        pa.id_produto,
        pa.nome,
        pa.codigo_barras,
        uv.data_ultima_venda,
        CASE 
            WHEN uv.data_ultima_venda IS NULL THEN 'Nunca vendeu'
            ELSE (EXTRACT(EPOCH FROM (NOW() - uv.data_ultima_venda)) / 86400)::NUMERIC(10,2) || ' dias'
        END AS dias_sem_venda
    FROM produtos_ativos pa
    LEFT JOIN ultima_venda_produto uv ON pa.id_produto = uv.id_produto
    WHERE uv.data_ultima_venda IS NULL 
    OR uv.data_ultima_venda < (NOW() - INTERVAL '90 days')
    ORDER BY uv.data_ultima_venda NULLS FIRST;
"""
df_estoque_parado = executar_query(query45)
print("Total de produtos com estoque parado há mais de 90 dias:")
print(df_estoque_parado.shape[0])
print("Produtos com estoque parado há mais de 90 dias:")
print(df_estoque_parado.head(15))  # Mostrar apenas os 15 primeiros

####
# Compare o valor do estoque atual com o mesmo período do ano anterior
####
print("\n")
print("\n")
print("#### PERGUNTA 46 ####")
query46 = """
    WITH estoque_atual AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    ),
    estoque_ano_anterior AS (
        SELECT DISTINCT ON (he.id_produto, he.id_loja)
            he.id_produto,
            he.id_loja,
            he.estoque,
            he.data_estoque
        FROM maloka_core.historico_estoque he
        WHERE he.data_estoque <= (CURRENT_DATE - INTERVAL '1 year')
        ORDER BY he.id_produto, he.id_loja, he.data_estoque DESC
    )
    SELECT
        SUM(ea.estoque * p.preco_venda) AS valor_total_atual,
        SUM(eaa.estoque * p.preco_venda) AS valor_total_ano_anterior
    FROM maloka_core.produto p
    LEFT JOIN estoque_atual ea 
        ON ea.id_produto = p.id_produto
    LEFT JOIN estoque_ano_anterior eaa 
        ON eaa.id_produto = p.id_produto
    WHERE p.ativo = TRUE
    AND p.excluido = FALSE;
"""
df_comparacao_estoque_parado = executar_query(query46)
print("Comparação do valor do estoque atual com o mesmo período do ano anterior:")
print(df_comparacao_estoque_parado)

# Coletar todos os resultados para salvar no arquivo
print("\n\nColetando resultados para arquivo...")

# Função para formatar dataframe como texto para incluir no relatório
def formatar_dataframe(df, max_linhas=20):
    if df is None or len(df) == 0:
        return ["Sem dados disponíveis"]
    
    # Limitar o número de linhas para não sobrecarregar o relatório
    if len(df) > max_linhas:
        df_resumido = df.head(max_linhas)
        return df_resumido.to_string(index=False).split('\n') + [f"... (mostrando {max_linhas} de {len(df)} linhas)"]
    else:
        return df.to_string(index=False).split('\n')

resultados = {
    "1 - Liste os produtos com estoque negativo classificados como B na curva ABC": [
        f"Total de produtos com estoque negativo classificados como B na curva ABC: {df_produtos_B.shape[0]}"
    ],

    "2 - Quais produtos da classe C na curva ABC estão negativos": [
        f"Total de produtos com estoque negativo classificados como C na curva ABC: {df_produtos_C.shape[0]}"
    ],

    "3 - Liste produtos com estoque negativo classificado como C na curva ABC. Mostre além do estoque atual, o valor de venda de cada um.": formatar_dataframe(df_produtos_C_valor),

    "4 - Quantas e quais são as categorias de produtos cadastradas?": [
        f"Total de categorias de produtos: {df_categorias['total_categorias'][0]}",
        f"Categorias de produtos: {df_categorias['categorias'][0]}"
    ],

    "5 - Quantas e quais são as segmentações de clientes?": [
        f"Total de segmentações de clientes: {df_segmentos['total_segmentos'][0]}",
        f"Segmentações de clientes: {df_segmentos['segmentos'][0]}"
    ],

    "6 - Compare o faturamento deste ano, até o último mês fechado, com o mesmo período no ano anterior": [
        f"Faturamento 2024 (até o último mês fechado): R$ {df_faturamento['faturamento_2024'][0]}",
        f"Faturamento 2025 (até o último mês fechado): R$ {df_faturamento['faturamento_2025'][0]}",
        f"Diferença de faturamento: R$ {df_faturamento['diferenca'][0]}",
        f"Variação percentual: {df_faturamento['variacao_percentual'][0]}%"
    ],

    "7 - Elabore uma tabela com a venda por grupo de mercadoria mostrando o crescimento ou decréscimo de vendas nos três últimos meses, incluindo o mês atual": formatar_dataframe(df_crescimento_mercadorias),

    f"8- Quais clientes compraram o produto {general_config['produtoX']} no último mês": formatar_dataframe(df_clientes),

    f"9 - Liste as vendas este ano do produto {general_config['produtoX']} até o dia de ontem": formatar_dataframe(df_vendas_produto),

    "10 - qual o ticket médio dos clientes campeões": [
        f"Ticket médio dos clientes campeões: R$ {df_ticket_medio['ticket_medio_campeoes'][0]}",
        f"Quantidade de vendas para campeões: {df_ticket_medio['quantidade_vendas_campeoes'][0]}",
        f"Valor total de vendas para campeões: R$ {df_ticket_medio['valor_total_vendas_campeoes'][0]}",
        f"Quantidade de clientes campeões com vendas: {df_ticket_medio['quantidade_clientes_campeoes'][0]}"
    ],

    "11 - qual o ticket médio geral de 2025": [
        f"Ticket médio geral de 2025: R$ {df_ticket_medio_2025['ticket_medio_2025'][0]}"
    ],

    "13 - calcule o valor médio de produtos vendidos para os clientes campeões": [
        f"Valor médio de produtos vendidos para os clientes campeões: R$ {df_valor_medio_produtos['preco_medio_geral'][0]}"
    ],

    "14 - qual o percentual de vendas no último mês para os clientes campeões por categoria de produtos": formatar_dataframe(df_percentual_vendas_categoria),

    "15 - gere um ranking de total de vendas da última semana por vendedor": formatar_dataframe(df_ranking_vendas),

    f"16 - gera as vendas de um vendedor {general_config['vendedor']} por categoria de material": formatar_dataframe(df_vendas_vendedor),

    "17 - liste em uma tabela o ticket médio de vendas por vendedor no mês passado": formatar_dataframe(df_ticket_medio_vendedor),

    "18 - gere as vendas de ontem por loja (bibi)": formatar_dataframe(df_vendas_ontem_loja),

    "19 - gere o ticket médio de vendas no mês passado por loja (bibi)": formatar_dataframe(df_ticket_medio_loja),

    "20 - liste um ranking com o total de vendas de ontem, por loja (bibi)": formatar_dataframe(df_vendas_ontem_loja),

    f"21 - liste 5 produtos da categoria {general_config['categoria']}": formatar_dataframe(df_produtos_categoria),

    "22 - liste 10 clientes campeões": formatar_dataframe(df_clientes_campeoes),

    "23 - liste 5 clientes sumidos": formatar_dataframe(df_clientes_sumidos),

    "24 - mostre os 10 produtos campeões em vendas na última semana": formatar_dataframe(df_campeoes_vendas),

    "25 - mostre o vendedor campeão em vendas no mês passado": [
        f"Vendedor: {df_vendedor_campeao['vendedor'][0]}",
        f"Total de vendas: R$ {df_vendedor_campeao['total_vendas'][0]}",
        f"Quantidade de vendas: {df_vendedor_campeao['quantidade_vendas'][0]}"
    ],

    "26 - Analise as vendas do último trimestre comparado ao mesmo período no ano anterior": [
        f"Total vendas trimestre atual: R$ {df_comparativo_trimestre['total_vendas_atual'][0]}",
        f"Total vendas trimestre ano anterior: R$ {df_comparativo_trimestre['total_vendas_anterior'][0]}",
        f"Diferença: R$ {df_comparativo_trimestre['diferenca'][0]}",
        f"Variação percentual: {df_comparativo_trimestre['variacao_percentual'][0]}%",
        f"Quantidade de vendas atual: {df_comparativo_trimestre['qtd_vendas_atual'][0]}",
        f"Quantidade de vendas anterior: {df_comparativo_trimestre['qtd_vendas_anterior'][0]}",
    ],

    "27 - Qual a venda de serviço este mês? (bibi)": [
        f"Quantidade de vendas: {df_venda_servico_mes['quantidade_vendas'][0]}",
        f"Total vendas de serviço: R$ {df_venda_servico_mes['total_vendas_servico'][0]}"
    ],

    "28 - Qual o ticket médio de serviço ? (bibi)": [
        f"Ticket médio de serviço: R$ {df_ticket_medio_servico['ticket_medio_servico'][0]}"
    ],

    "29 - liste o ranking de vendas por loja e por vendedor em agosto de 2025": formatar_dataframe(df_ranking_vendas),

    f"30 - Analise as vendas da categoria de produtos {general_config['categoria']} no último trimestre e indique ações para melhorar as vendas": formatar_dataframe(df_analise_categoria),

    "31 - Quais produtos estão com estoque zerado?": [
        f"Total de produtos com estoque zerado: {df_produtos_zerados.shape[0]}"
    ],

    "32 - Liste os 20 produtos com maior quantidade em estoque": formatar_dataframe(df_top_estoque),

    "34 - Quais produtos não tiveram movimentação de estoque nos últimos 30 dias?": [
        f"Total de produtos sem movimentação de estoque nos últimos 30 dias: {df_estoque_sem_movimentacao.shape[0]}"
    ],
    
    "36 - Mostre a movimentação de estoque de ontem por tipo (entrada/saída)": formatar_dataframe(df_movimentacao_ontem),
    
    "37 - Quais foram as últimas 10 entradas de estoque por compra?": formatar_dataframe(df_ultimas_entradas),
    
    "40 - Quais produtos tiveram mais entradas de estoque no último trimestre?": formatar_dataframe(df_produtos_mais_entradas),
    
    "41 - Compare as entradas vs saídas de estoque do mês passado": [
        f"Total de entradas: {df_entradas_vs_saidas['total_entradas'][0]}",
        f"Total de saídas: {df_entradas_vs_saidas['total_saidas'][0]}",
        f"Saldo líquido: {df_entradas_vs_saidas['saldo_liquido'][0]}"
    ],
    
    "42 - Qual o valor total do estoque atual (baseado no preço de custo)?": [
        f"Valor total do estoque (baseado no preço de custo): R$ {df_valor_estoque['valor_total'][0]}"
    ],
    
    "43 - Qual o valor total do estoque atual (baseado no preço de venda)?": [
        f"Valor total do estoque (baseado no preço de venda): R$ {df_valor_estoque_venda['valor_total'][0]}"
    ],
    
    "44 - Liste os 10 produtos que representam maior valor em estoque": formatar_dataframe(df_produtos_maior_valor_estoque),
    
    "45 - Mostre produtos sem vendas há mais de 90 dias": formatar_dataframe(df_estoque_parado),
    
    "46 - Compare o valor total do custo do estoque atual com o mesmo período do ano anterior": formatar_dataframe(df_comparacao_estoque_parado)
}

# Gerar nome do arquivo com base no banco de dados e data atual
data_atual = datetime.now().strftime("%Y%m%d")
nome_arquivo_base = f"{general_config['database']}_{data_atual}"

# Incluir informações sobre o banco de dados e a data da consulta
data_formatada = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
resultados["#### Informações do Relatório ####"] = [
    f"Banco de Dados: {general_config['database']}",
    f"Data e Hora: {data_formatada}",
    f"Consulta realizada em: {os.getenv('DB_HOST')}"
]

# Tentar salvar como PDF
try:
    nome_arquivo_pdf = f"{nome_arquivo_base}.pdf"
    sucesso_pdf = salvar_resultados_pdf(resultados, nome_arquivo_pdf)
        
except Exception as e:
    print(f"Erro ao salvar resultados em PDF: {e}")
    sucesso_pdf = None
