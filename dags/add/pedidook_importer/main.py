from config.db_connection import execute_migration
from datetime import datetime, timedelta
from endpoints.pedido_ok_importar_clientes import importar_clientes
from endpoints.pedido_ok_importar_fornecedores import importar_fornecedores
from endpoints.pedido_ok_importar_pedidos import importar_pedidos 
from endpoints.pedido_ok_importar_vendedores import importar_vendedores
from endpoints.pedido_ok_importar_produtos import importar_produtos
from endpoints.pedido_ok_importar_relatorio_estoque import importar_relatorio_estoque

def importar_todos_endpoints(last_modified_date):
    
    print(f"Iniciando a importação de todos os dados desde {last_modified_date}...")

    # Importação de relatorio de estoque
    print("Importando relatório de estoque...")
    #importar_relatorio_estoque()

    # Importação de clientes
    print("Importando clientes...")
    importar_clientes(last_modified_date)

    # Importação de fornecedores
    print("Importando fornecedores...")
    importar_fornecedores(last_modified_date)

    # Importação de produtos
    print("Importando produtos...")
    importar_produtos()

    # Importação de pedidos
    print("Importando pedidos...")
    importar_pedidos(last_modified_date)

    # Importação de vendedores
    print("Importando vendedores...")
    importar_vendedores(last_modified_date)

    print("Importação de todos os dados concluída.")

    # Executa a migração de dados após a importação
    print("Executando migração de dados para o modelo padrão...")
    execute_migration()

if __name__ == "__main__":
    # Define a data de modificação como 5 dias atrás
    last_modified_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S')
    importar_todos_endpoints(last_modified_date)
