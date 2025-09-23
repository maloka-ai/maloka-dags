# Logs no Airflow - Guia de Uso

O Airflow fornece um sistema integrado para registrar logs de suas DAGs e tarefas. Este guia explica como usar esse sistema de logging corretamente em nossas DAGs.

## Introdução

Logs são essenciais para monitorar e depurar o funcionamento das DAGs. O Airflow captura automaticamente os logs das tarefas e os exibe na interface web, permitindo fácil acesso e análise.

## Como Usar o Sistema de Logs

1. **Importação do Módulo de Logging**

```python
from utils.airflow_logging import configurar_logger, log_task_info

# Configuração do logger
logger = configurar_logger(__name__)
```

2. **Registrando Logs Simples**

```python
# Em nível de DAG (fora das tarefas)
logger.info("Esta mensagem aparecerá nos logs gerais do Airflow")
logger.error("Este erro aparecerá nos logs gerais do Airflow")
```

3. **Registrando Logs em Tarefas com Contexto**

```python
def minha_tarefa(**kwargs):
    # Usa o contexto para incluir informações da tarefa no log
    log_task_info(kwargs, "Iniciando tarefa", nivel="info")
    
    # ...código da tarefa...
    
    log_task_info(kwargs, "Tarefa concluída com sucesso")
    
    # Em caso de erro
    try:
        # código que pode falhar
        pass
    except Exception as e:
        log_task_info(kwargs, f"Erro na tarefa: {str(e)}", nivel="error")
        raise  # Re-lança para o Airflow saber que a tarefa falhou
```

4. **Configurando a Tarefa com Contexto**

```python
tarefa = PythonOperator(
    task_id='minha_tarefa',
    python_callable=minha_tarefa,
    provide_context=True  # Importante para passar o contexto para a função
)
```

## Níveis de Log Disponíveis

- **info**: Para informações gerais e fluxo normal de execução
- **warning**: Para situações que não são erros, mas podem se tornar
- **error**: Para erros que afetam a execução
- **debug**: Para informações detalhadas úteis na depuração

## Visualizando os Logs

Os logs podem ser visualizados na interface web do Airflow:

1. Acesse a visualização de DAGs
2. Clique na DAG específica
3. Clique na execução específica
4. Clique na tarefa específica
5. Clique na aba "Log"

## Melhores Práticas

1. **Seja Específico**: Inclua detalhes suficientes para entender o que está acontecendo
2. **Use Níveis Apropriados**: Use o nível correto para cada mensagem
3. **Registre Início e Fim**: Sempre registre quando uma tarefa inicia e termina
4. **Capture Exceções**: Sempre registre exceções antes de relançá-las
5. **Inclua Contexto**: Use a função `log_task_info` para incluir o contexto da tarefa

## Exemplos Completos

Veja a implementação em `dags/modelagens/beny_dag.py` para um exemplo completo de uso do sistema de logs.
