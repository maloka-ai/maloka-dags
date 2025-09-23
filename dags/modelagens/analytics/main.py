#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import importlib
import traceback
import logging
import calendar
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from dags.modelagens.analytics.config_clientes import CLIENTES

# Importar o sistema de logging personalizado para o Airflow
try:
    from utils.airflow_logging import configurar_logger, log_task_info
    AIRFLOW_LOGGER_DISPONIVEL = True
except ImportError:
    AIRFLOW_LOGGER_DISPONIVEL = False
    # Fallback para logging padrão caso não estejamos no ambiente do Airflow

class ModelagemManager:
    """
    Classe responsável por gerenciar e atualizar todas as modelagens do sistema.
    Permite executar modelagens individualmente ou em grupos conforme necessário.
    Respeita as dependências entre modelagens para garantir ordem correta de execução.
    """
    
    def __init__(self, airflow_context=None):
        """
        Inicializa o gerenciador de modelagens
        
        Args:
            airflow_context (dict, optional): Contexto do Airflow, fornecido por kwargs na execução da task
        """
        self.diretorio_base = os.path.dirname(os.path.abspath(__file__))
        self.airflow_context = airflow_context
        
        # Configura o logger
        self.configurar_logger()
        
        # Mapeia todas as modelagens disponíveis
        self.modelagens = {
            'cliente': {
                'segmentacao': 'dags.modelagens.analytics.cliente.analise_segmentacao',
                'retencao': 'dags.modelagens.analytics.cliente.recorrencia_retencao',
                'previsao': 'dags.modelagens.analytics.cliente.previsao_retorno',
            },
            'compra': {
                'analise': 'dags.modelagens.analytics.compra.analise_compra',
            },
            'estoque': {
                'analise': 'dags.modelagens.analytics.estoque.analise_estoque',
            },
            'vendas': {
                'faturamento': 'dags.modelagens.analytics.vendas.analise_faturamento',
                'atipicas': 'dags.modelagens.analytics.vendas.analise_vendas_atipicas',
                # 'cesta': 'dags.modelagens.analytics.vendas.cesta_compras',
                # 'cross': 'modelagens.analytics.vendas.cross_selling',
            }
        }
        
        # Define a ordem de execução das modelagens
        # Formato: [(categoria, nome), ...] - a ordem dos itens define a sequência de execução
        self.ordem_execucao = [
            # Primeiro as modelagens sem dependências
            ('vendas', 'faturamento'),
            ('estoque', 'analise'),
            ('cliente', 'segmentacao'),  # Possui regra especial (primeiro dia do mês)
            ('cliente', 'retencao'),
            ('cliente', 'previsao'),
            ('compra', 'analise'),
            
            # Modelagens com dependências
            ('vendas', 'atipicas'),  # Depende de análise de compra
            
            # Outras modelagens que podem ser ativadas no futuro
            # ('vendas', 'cesta'),
            # ('vendas', 'cross'),
        ]
        
        # Mapa de dependências para validação - apenas as que realmente possuem dependências
        self.dependencias = {
            # Única modelagem com dependência
            ('vendas', 'atipicas'): [('compra', 'analise')],
        }
    
    def configurar_logger(self):
        """Configura o sistema de logging, usando o Airflow quando disponível"""
        if AIRFLOW_LOGGER_DISPONIVEL:
            # Usar o logger do Airflow se disponível
            self.logger = configurar_logger('ModelagemManager')
            self.log_info = self._log_airflow_info
            self.log_warning = self._log_airflow_warning
            self.log_error = self._log_airflow_error
        else:
            # Fallback para logging padrão
            log_dir = os.path.join(self.diretorio_base, 'logs')
            os.makedirs(log_dir, exist_ok=True)
            
            # Nome do arquivo de log com timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = os.path.join(log_dir, f'atualizacao_{timestamp}.log')
            
            # Configuração do logger
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file, encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            self.logger = logging.getLogger('ModelagemManager')
            self.log_info = self._log_standard_info
            self.log_warning = self._log_standard_warning
            self.log_error = self._log_standard_error
    
    def _log_airflow_info(self, mensagem):
        """Log info usando o sistema do Airflow"""
        if self.airflow_context:
            log_task_info(self.airflow_context, mensagem, nivel="info")
        else:
            self.logger.info(mensagem)
            print(mensagem)  # Para garantir visibilidade no console
    
    def _log_airflow_warning(self, mensagem):
        """Log warning usando o sistema do Airflow"""
        if self.airflow_context:
            log_task_info(self.airflow_context, mensagem, nivel="warning")
        else:
            self.logger.warning(mensagem)
            print(f"AVISO: {mensagem}")
    
    def _log_airflow_error(self, mensagem):
        """Log error usando o sistema do Airflow"""
        if self.airflow_context:
            log_task_info(self.airflow_context, mensagem, nivel="error")
        else:
            self.logger.error(mensagem)
            print(f"ERRO: {mensagem}")
    
    def _log_standard_info(self, mensagem):
        """Log info usando o sistema padrão"""
        self.logger.info(mensagem)
        print(mensagem)
    
    def _log_standard_warning(self, mensagem):
        """Log warning usando o sistema padrão"""
        self.logger.warning(mensagem)
        print(f"AVISO: {mensagem}")
    
    def _log_standard_error(self, mensagem):
        """Log error usando o sistema padrão"""
        self.logger.error(mensagem)
        print(f"ERRO: {mensagem}")
    
    def listar_modelagens(self):
        """Lista todas as modelagens disponíveis"""
        self.log_info("Modelagens disponíveis:")
        
        for categoria, modelos in self.modelagens.items():
            self.log_info(f"\n[{categoria.upper()}]")
            for nome, caminho in modelos.items():
                self.log_info(f"  - {nome}: {caminho}")
    
    def verificar_regras_especiais(self, categoria, nome):
        """
        Verifica regras especiais para execução de certas modelagens
        
        Args:
            categoria (str): Categoria da modelagem
            nome (str): Nome da modelagem
        
        Returns:
            tuple: (pode_executar, mensagem) - Tupla indicando se pode executar e mensagem de razão
        """
        # Regra especial para segmentação: só executa no primeiro dia do mês
        if categoria == 'cliente' and nome == 'segmentacao':
            hoje = datetime.now()
            if hoje.day != 1:  # Verifica se não é o primeiro dia do mês
                return False, f"Segmentação só pode ser executada no primeiro dia do mês. Hoje é dia {hoje.day}."
        
        # Se não houver restrição, retorna True
        return True, ""
    
    def executar_modelagem(self, categoria, nome, ignorar_regras=False, cliente_especifico=None):
        """
        Executa uma modelagem específica
        
        Args:
            categoria (str): Categoria da modelagem (cliente, compra, estoque, vendas)
            nome (str): Nome da modelagem específica
            ignorar_regras (bool): Se True, ignora as regras especiais de execução
            cliente_especifico (str, optional): ID do cliente específico para executar a modelagem.
                                                Se None, executa para todos os clientes.
        
        Returns:
            bool: True se executou com sucesso, False caso contrário
        """
        try:
            if categoria not in self.modelagens or nome not in self.modelagens[categoria]:
                self.log_error(f"Modelagem não encontrada: {categoria}.{nome}")
                return False
            
            # Verifica regras especiais, a menos que esteja configurado para ignorá-las
            if not ignorar_regras:
                pode_executar, mensagem = self.verificar_regras_especiais(categoria, nome)
                if not pode_executar:
                    self.log_warning(f"Não foi possível executar {categoria}.{nome}: {mensagem}")
                    return False
            
            modulo_path = self.modelagens[categoria][nome]
            self.log_info(f"Executando {modulo_path}...")
            
            # Importa o módulo
            modulo = importlib.import_module(modulo_path)
            
            # Mapeamento das funções principais em cada módulo
            funcoes_principais = {
                'dags.modelagens.analytics.vendas.analise_faturamento': 'gerar_relatorios_faturamento',
                'dags.modelagens.analytics.estoque.analise_estoque': 'gerar_relatorios_estoque',
                'dags.modelagens.analytics.cliente.analise_segmentacao': 'gerar_analise_cliente',
                'dags.modelagens.analytics.cliente.recorrencia_retencao': 'gerar_analise_recorrencia_retencao',
                'dags.modelagens.analytics.cliente.previsao_retorno': 'gerar_analise_previsao_retorno',
                'dags.modelagens.analytics.compra.analise_compra': 'gerar_relatorios_compra',
                'dags.modelagens.analytics.vendas.analise_vendas_atipicas': 'gerar_analise_vendas_atipicas',
                # 'dags.modelagens.analytics.vendas.cesta_compras': 'gerar_analise_cesta_compras',
                # 'dags.modelagens.analytics.vendas.cross_selling': 'gerar_analise_cross_selling'
            }
            
            # Verifica se há uma função principal específica para este módulo
            if modulo_path in funcoes_principais:
                nome_funcao = funcoes_principais[modulo_path]
                if hasattr(modulo, nome_funcao):
                    if cliente_especifico:
                        # Executa a função apenas para o cliente específico
                        if cliente_especifico in CLIENTES:
                            self.log_info(f"Executando {nome_funcao} para o cliente {cliente_especifico}")
                            getattr(modulo, nome_funcao)(cliente_especifico)
                        else:
                            self.log_error(f"Cliente {cliente_especifico} não encontrado na configuração")
                            return False
                    else:
                        # Executa a função para cada cliente
                        for cliente in CLIENTES.keys():
                            self.log_info(f"Executando {nome_funcao} para o cliente {cliente}")
                            getattr(modulo, nome_funcao)(cliente)
                else:
                    self.log_warning(f"Função {nome_funcao} não encontrada no módulo {modulo_path}")
            # Verifica se há uma função main ou run no módulo
            elif hasattr(modulo, 'main'):
                # Para funções main/run, não temos como passar o cliente específico 
                # a menos que seja modificado o código dessas funções
                self.log_warning(f"Módulo {modulo_path} tem função main() mas não suporta cliente específico")
                modulo.main()
            elif hasattr(modulo, 'run'):
                self.log_warning(f"Módulo {modulo_path} tem função run() mas não suporta cliente específico")
                modulo.run()
            else:
                # Assume que o módulo executa automaticamente ao ser importado
                self.log_info(f"Módulo {modulo_path} não tem função principal mapeada, main() ou run()")
            
            self.log_info(f"Execução de {modulo_path} concluída com sucesso!")
            return True
            
        except Exception as e:
            self.log_error(f"Erro ao executar {categoria}.{nome}: {str(e)}")
            self.log_error(traceback.format_exc())
            return False
    
    def executar_categoria(self, categoria):
        """
        Executa todas as modelagens de uma categoria
        
        Args:
            categoria (str): Categoria das modelagens a executar
        
        Returns:
            dict: Resultados de execução para cada modelagem
        """
        if categoria not in self.modelagens:
            self.log_error(f"Categoria não encontrada: {categoria}")
            return {}
        
        self.log_info(f"Executando todas as modelagens da categoria: {categoria}")
        resultados = {}
        
        for nome in self.modelagens[categoria]:
            resultados[nome] = self.executar_modelagem(categoria, nome)
        
        return resultados
    
    def atualizar_tudo(self, cliente_especifico=None):
        """
        Executa todas as modelagens seguindo a ordem definida de dependências
        
        Args:
            cliente_especifico (str, optional): ID do cliente específico para executar as modelagens.
                                                Se None, executa para todos os clientes.
        
        Returns:
            dict: Resultados de execução para cada modelagem
        """
        if cliente_especifico:
            if cliente_especifico not in CLIENTES:
                self.log_error(f"Cliente {cliente_especifico} não encontrado na configuração")
                return {}
            self.log_info(f"Iniciando atualização de todas as modelagens na ordem definida para o cliente {cliente_especifico}")
        else:
            self.log_info("Iniciando atualização de todas as modelagens na ordem definida para todos os clientes")
        resultados = {}
        
        # Inicializa o dicionário de resultados
        for categoria in self.modelagens:
            if categoria not in resultados:
                resultados[categoria] = {}
        
        # Executa as modelagens na ordem definida
        for categoria, nome in self.ordem_execucao:
            # Verifica se as dependências foram executadas com sucesso
            if (categoria, nome) in self.dependencias:
                for dep_cat, dep_nome in self.dependencias[(categoria, nome)]:
                    # Se a dependência existe mas falhou, pula esta modelagem
                    if dep_cat in resultados and dep_nome in resultados[dep_cat] and not resultados[dep_cat][dep_nome]:
                        self.log_warning(f"Pulando {categoria}.{nome} porque a dependência {dep_cat}.{dep_nome} falhou")
                        resultados.setdefault(categoria, {})[nome] = False
                        continue
            
            # Executa a modelagem atual
            if categoria not in resultados:
                resultados[categoria] = {}
            resultados[categoria][nome] = self.executar_modelagem(categoria, nome, cliente_especifico=cliente_especifico)
        
        # Exibe um resumo dos resultados
        self.exibir_resumo(resultados)
        return resultados
    
    def exibir_resumo(self, resultados):
        """
        Exibe um resumo dos resultados da execução
        
        Args:
            resultados (dict): Resultados da execução
        """
        self.log_info("\n" + "="*50)
        self.log_info("RESUMO DA EXECUÇÃO")
        self.log_info("="*50)
        
        sucessos = 0
        falhas = 0
        
        for categoria, modelos in resultados.items():
            self.log_info(f"\n[{categoria.upper()}]")
            
            for nome, resultado in modelos.items():
                status = "✅ SUCESSO" if resultado else "❌ FALHA"
                self.log_info(f"  - {nome}: {status}")
                
                if resultado:
                    sucessos += 1
                else:
                    falhas += 1
        
        self.log_info("\n" + "="*50)
        self.log_info(f"TOTAL: {sucessos + falhas} modelagens")
        self.log_info(f"SUCESSOS: {sucessos}")
        self.log_info(f"FALHAS: {falhas}")
        self.log_info("="*50)


    def verificar_dependencias(self, categoria, nome):
        """
        Verifica se as dependências de uma modelagem estão na ordem correta
        
        Args:
            categoria (str): Categoria da modelagem
            nome (str): Nome da modelagem
        
        Returns:
            list: Lista de dependências que precisam ser executadas antes
        """
        # Se não tem dependências, retorna lista vazia
        if (categoria, nome) not in self.dependencias:
            return []
            
        # Retorna a lista de dependências
        return self.dependencias[(categoria, nome)]
    
    def executar_com_dependencias(self, categoria, nome, ignorar_regras=False, cliente_especifico=None):
        """
        Executa uma modelagem e suas dependências na ordem correta
        
        Args:
            categoria (str): Categoria da modelagem
            nome (str): Nome da modelagem
            ignorar_regras (bool): Se True, ignora as regras especiais de execução
            cliente_especifico (str, optional): ID do cliente específico para executar a modelagem.
                                                Se None, executa para todos os clientes.
        
        Returns:
            bool: True se executou com sucesso, False caso contrário
        """
        self.log_info(f"Executando {categoria}.{nome} com suas dependências")
        
        # Verifica regras especiais para a modelagem atual
        if not ignorar_regras:
            pode_executar, mensagem = self.verificar_regras_especiais(categoria, nome)
            if not pode_executar:
                self.log_warning(f"Não foi possível executar {categoria}.{nome}: {mensagem}")
                return False
        
        # Verifica as dependências
        dependencias = self.verificar_dependencias(categoria, nome)
        resultados_dep = {}
        
        # Executa as dependências primeiro
        for dep_cat, dep_nome in dependencias:
            self.log_info(f"Executando dependência {dep_cat}.{dep_nome} para {categoria}.{nome}")
            
            # Verificar se a dependência é segmentação e se estamos ignorando regras para modelagem atual
            # Se for segmentação e a modelagem atual está sendo forçada, também ignoramos as regras para a segmentação
            ignorar_regras_dep = ignorar_regras and (dep_cat == 'cliente' and dep_nome == 'segmentacao')
            
            resultado_dep = self.executar_com_dependencias(dep_cat, dep_nome, 
                                               ignorar_regras=ignorar_regras_dep,
                                               cliente_especifico=cliente_especifico)
            
            if not resultado_dep:
                self.log_warning(f"Dependência {dep_cat}.{dep_nome} falhou, pulando {categoria}.{nome}")
                return False
        
        # Executa a modelagem atual
        return self.executar_modelagem(categoria, nome, ignorar_regras=ignorar_regras, 
                                      cliente_especifico=cliente_especifico)

    def mostrar_ordem_execucao(self):
        """Mostra a ordem de execução das modelagens com suas dependências"""
        self.log_info("Ordem de execução das modelagens:")
        
        self.log_info("\nLista de modelagens na ordem de execução:")
        self.log_info("(Atualmente apenas 'vendas.atipicas' depende de 'compra.analise')\n")
        
        for i, (categoria, nome) in enumerate(self.ordem_execucao, 1):
            deps = self.verificar_dependencias(categoria, nome)
            
            # Destacar se a modelagem tem regras especiais
            regra_especial = ""
            if categoria == 'cliente' and nome == 'segmentacao':
                regra_especial = " [⚠️ Requer primeiro dia do mês]"
                
            deps_str = ", ".join([f"{c}.{n}" for c, n in deps]) if deps else "nenhuma"
            
            self.log_info(f"{i}. {categoria.upper()}.{nome}{regra_especial} - Dependências: {deps_str}")


def main():
    """Função principal para execução via linha de comando"""
    manager = ModelagemManager()
    
    if len(sys.argv) == 1:
        # Sem argumentos: mostra ajuda
        print("Uso:")
        print("  python main.py listar                       - Lista todas as modelagens disponíveis")
        print("  python main.py ordem                        - Mostra a ordem de execução das modelagens")
        print("  python main.py tudo                         - Atualiza todas as modelagens na ordem correta")
        print("  python main.py categoria <nome>             - Atualiza todas as modelagens de uma categoria")
        print("  python main.py modelagem <categoria> <nome> - Atualiza uma modelagem específica com suas dependências")
        print("  python main.py forcar <categoria> <nome>    - Força a execução de uma modelagem ignorando regras especiais")
        print("  python main.py regras                       - Mostra as regras especiais de execução das modelagens")
        return
    
    comando = sys.argv[1].lower()
    
    if comando == "listar":
        manager.listar_modelagens()
    
    elif comando == "tudo":
        manager.atualizar_tudo()
    
    elif comando == "categoria" and len(sys.argv) > 2:
        categoria = sys.argv[2].lower()
        manager.executar_categoria(categoria)
    
    elif comando == "ordem":
        manager.mostrar_ordem_execucao()
    
    elif comando == "modelagem" and len(sys.argv) > 3:
        categoria = sys.argv[2].lower()
        nome = sys.argv[3].lower()
        manager.executar_com_dependencias(categoria, nome)
    
    elif comando == "forcar" and len(sys.argv) > 3:
        categoria = sys.argv[2].lower()
        nome = sys.argv[3].lower()
        print(f"Forçando execução de {categoria}.{nome} sem verificar regras especiais...")
        manager.executar_modelagem(categoria, nome, ignorar_regras=True)
    
    elif comando == "regras":
        # Mostrar as regras especiais configuradas
        print("\n=== REGRAS ESPECIAIS DE EXECUÇÃO ===")
        print("- Segmentação de clientes: só pode ser executada no primeiro dia do mês")
        # Adicione outras regras aqui quando forem criadas
        
        # Mostrar informações sobre dependências
        print("\n=== DEPENDÊNCIAS ENTRE MODELAGENS ===")
        print("- Vendas Atípicas: depende de Análise de Compra")
        
        # Verificar quais regras estão ativas hoje
        hoje = datetime.now()
        print(f"\nHOJE: {hoje.strftime('%d/%m/%Y')} (Dia {hoje.day})")
        print("\nModelos que NÃO podem ser executados hoje:")
        
        if hoje.day != 1:
            print("- cliente.segmentacao (requer primeiro dia do mês)")
            print("  └─ Pode ser executado usando 'python main.py forcar cliente segmentacao'")
        else:
            print("- Todas as modelagens podem ser executadas hoje!")
    
    else:
        print("Comando inválido. Use 'python main.py' para ver as opções disponíveis.")


if __name__ == "__main__":
    main()
