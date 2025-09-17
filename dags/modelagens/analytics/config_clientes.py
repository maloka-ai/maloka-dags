"""
Configurações de clientes para conexão ao banco de dados e parâmetros de análise RFM (Recência, Frequência, Monetário).
"""

CLIENTES = {
    "add": {
        "database": "add",
        "schema": "maloka_core",

        ####################
        # SEGMENTACAO RFMA
        ####################
        # Indicador de separação PF/PJ
        "separar_pf_pj": False,  # Não separar análise para PF e PJ
        #recencia
        "recencia_recentes": 180, # 6 meses
        "recencia_fieis": 180,    # 6 meses
        "recencia_campeoes": 180, # 6 meses
        #frequencia
        "frequencia_recentes": 1,
        "frequencia_fieis": 4,
        "frequencia_campeoes": 34,
        #decil monetario
        "decil_monetario_recentes": 6,
        "decil_monetario_fieis": 8,
        "decil_monetario_campeoes": 10,
        #idade
        "idade_fieis": 730,       # 2 anos
        #inatividade e sumidos
        "sumidos": 180,     # 6 meses
        "inatividade": 730, # 2 anos

        ## Cliente ADD
        "descricao_regras":"Novos Clientes: cliente fez primeira compra recentemente, nos últimos 30 dias (quando a última compra foi há no máximo 30 dias e é cliente há no máximo 30 dias). Campeões: compraram recentemente (últimos 6 meses), com muitas compras (34 ou mais pedidos) e gastam muito (estão no top 10% dos que mais gastam). Fiéis Alto Valor: clientes antigos da casa (são clientes há 2 anos ou mais), ainda ativos (compraram nos últimos 6 meses), fazem compras regulares (4 ou mais pedidos) e gastam bem acima da média (estão entre os 20% que mais gastam). Fiéis Baixo Valor: clientes antigos da casa (são clientes há 2 anos ou mais), ainda ativos (compraram nos últimos 6 meses), fazem compras regulares (4 ou mais pedidos) mas gastam valores medianos (até 80% do valor monetário). Recentes Alto Valor: compraram recentemente (últimos 6 meses), qualquer quantidade de pedidos, mas gastam acima da média (estão entre os 40% que mais gastam). Recentes Baixo Valor: compraram recentemente (últimos 6 meses), qualquer quantidade de pedidos, mas gastam valores baixos (estão entre os 60% que menos gastam). Sumidos: pararam de comprar temporariamente (entre 6 meses e 2 anos sem comprar). Inativos: inativos na loja (mais de 2 anos sem comprar).",

        ####################
        # CROSS_SELLING
        ####################
        "usar_cross_selling_produtos": False,  # Usar análise de cross-selling
        "min_support": 0.03,
        "min_confidence": 0.3,
        "tempo_analise_anos": 3,

        ####################
        # VENDA_ATIPICA
        ####################
        "analise_varejo": False,
        "podar_populares": False,
        "quantidade_populares": 0,
        "tempo_analise_atipicidade": 1,  # Tempo de análise para venda atípica em anos
    },
    "bibicell": {
        "database": "bibicell",
        "schema": "maloka_core",
        # Indicador de separação PF/PJ
        "separar_pf_pj": True,  # Separar análise para PF e PJ
        #recencia
        "recencia_recentes": 365, #  1 ano
        "recencia_fieis": 365,    #  1 ano
        "recencia_campeoes": 180, # 6 meses
        #frequencia
        "frequencia_recentes": 1,
        "frequencia_fieis": 3,
        "frequencia_campeoes": 3,
        #decil monetario
        "decil_monetario_recentes": 6,
        "decil_monetario_fieis": 7,
        "decil_monetario_campeoes": 10,
        #idade
        "idade_fieis": 730,       # 2 anos
        #inatividade e sumidos
        "sumidos": 365,     # 1 ano
        "inatividade": 730, # 2 anos

        ## Cliente Bibicell  
        "descricao_regras": "Novos Clientes: cliente fez primeira compra recentemente, nos últimos 30 dias (quando a última compra foi há no máximo 30 dias e é cliente há no máximo 30 dias). Campeões: compraram recentemente (últimos 6 meses) e gastam muito (estão no top 10% dos que mais gastam). Fiéis Alto Valor: clientes antigos da casa (são clientes há 2 anos ou mais), ainda ativos (compraram no último ano), fazem compras regulares (3 ou mais pedidos) e gastam bem acima da média (estão entre os 30% que mais gastam). Fiéis Baixo Valor: clientes antigos da casa (são clientes há 2 anos ou mais), ainda ativos (compraram no último ano), fazem compras regulares (3 ou mais pedidos) mas gastam valores medianos (até 70% do valor monetário). Recentes Alto Valor: compraram no último ano, qualquer quantidade de pedidos, mas gastam acima da média (estão entre os 40% que mais gastam). Recentes Baixo Valor: compraram no último ano, qualquer quantidade de pedidos, mas gastam valores baixos (estão entre os 60% que menos gastam). Sumidos: pararam de comprar temporariamente (entre 1 ano e 2 anos sem comprar). Inativos: inativos na loja (mais de 2 anos sem comprar).",


        ####################
        # CROSS_SELLING
        ####################
        "usar_cross_selling_produtos": True,  # Usar análise de cross-selling
        "podar_populares": True,  # Podar produtos populares
        "quantidade_populares": 3,  # Quantidade de produtos populares a serem considerados
        "min_support": 0.0001,
        "min_confidence": 0.05,
        "tempo_analise_anos": 1,

        ####################
        # VENDA_ATIPICA
        ####################
        "analise_varejo": True,
        "minimo_de_atipicidade": 5,        #quantidade mínima de atipicidade
        "valor_ruptura_estoque": 0,        #cobertura em dias que determina a atipicidade
        "tempo_metodo_empirico": 30,       #tempo em dias
        "tempo_analise_estatistico": 365,  # Tempo de análise para venda atípica em dias
        "tempo_analise_atipicidade": 1,
        "valor_desvio_padrao": 0.5,        # Valor do desvio padrão para identificar vendas atípicas 0.5 ou 2.0 para bibicell
    },
    "beny": {
        "database": "beny",
        "schema": "maloka_core",
        # Indicador de separação PF/PJ
        "separar_pf_pj": True,  # Separar análise para PF e PJ
        #recencia
        "recencia_recentes": 180, #  1 ano
        "recencia_fieis": 180,    #  1 ano
        "recencia_campeoes": 180, # 6 meses
        #frequencia
        "frequencia_recentes": 1,
        "frequencia_fieis": 4,
        "frequencia_campeoes": 7,
        #decil monetario
        "decil_monetario_recentes": 6,
        "decil_monetario_fieis": 8,
        "decil_monetario_campeoes": 10,
        #idade
        "idade_fieis": 730,       # 2 anos
        #inatividade e sumidos
        "sumidos": 180,          # 6 meses
        "inatividade": 360,      # 1 ano

        # Cliente Beny  
        "descricao_regras": "Novos Clientes: cliente fez primeira compra recentemente, nos últimos 30 dias (quando a última compra foi há no máximo 30 dias e é cliente há no máximo 30 dias). Campeões: compraram recentemente (últimos 6 meses) e gastam muito (estão no top 10% dos que mais gastam). Fiéis Alto Valor: clientes antigos da casa (são clientes há 2 anos ou mais), ainda ativos (compraram nos últimos 6 meses), fazem compras regulares (4 ou mais pedidos) e gastam bem acima da média (estão entre os 20% que mais gastam). Fiéis Baixo Valor: clientes antigos da casa (são clientes há 2 anos ou mais), ainda ativos (compraram nos últimos 6 meses), fazem compras regulares (4 ou mais pedidos) mas gastam valores medianos (até 80% do valor monetário). Recentes Alto Valor: compraram no último ano, qualquer quantidade de pedidos, mas gastam acima da média (estão entre os 40% que mais gastam). Recentes Baixo Valor: compraram no último ano, qualquer quantidade de pedidos, mas gastam valores baixos (estão entre os 60% que menos gastam). Sumidos: pararam de comprar temporariamente (entre 6 meses e 1 ano sem comprar). Inativos: inativos na loja (mais de 1 ano sem comprar).",


        ####################
        # CROSS_SELLING
        ####################
        "usar_cross_selling_produtos": True,  # Usar análise de cross-selling
        "podar_populares": True,  # Podar produtos populares
        "quantidade_populares": 3,  # Quantidade de produtos populares a serem considerados
        "min_support": 0.0001,
        "min_confidence": 0.05,
        "tempo_analise_anos": 1,

        ####################
        # VENDA_ATIPICA
        ####################
        "analise_varejo": True,
        "minimo_de_atipicidade": 5,       #quantidade mínima de atipicidade
        "valor_ruptura_estoque": 0,       #cobertura em dias que determina a atipicidade
        "tempo_metodo_empirico": 30,      #tempo em dias
        "tempo_analise_estatistico": 365, # Tempo de análise para venda atípica em dias
        "tempo_analise_atipicidade": 1,
        "valor_desvio_padrao": 0.5,       # Valor do desvio padrão para identificar vendas atípicas 0.5 ou 2.0 para bibicell
    },
    # Adicione outros clientes conforme necessário
}