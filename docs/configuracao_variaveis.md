# Configuração de Variáveis no Airflow

Este documento explica como as variáveis de configuração são gerenciadas no projeto, especialmente para conexões de banco de dados.

## Visão Geral da Solução

O sistema utiliza uma abordagem em camadas para buscar configurações de banco de dados:

1. **DAG de Variáveis**: A DAG `dag_load_variables` executa periodicamente (a cada hora) para carregar as variáveis do Airflow e disponibilizá-las via XCom.

2. **Sistema de Fallback**: Os módulos `database.py` e `triggers.py` possuem um mecanismo de fallback que tenta obter as configurações de várias fontes:
   - Diretamente do módulo `airflow_variables.py`
   - Do XCom da DAG `dag_load_variables`
   - Das variáveis de ambiente no sistema

## Como Funciona

### DAG de Variáveis

A DAG `dag_load_variables` tem as seguintes características:

- Executa a cada hora para manter os dados atualizados
- Busca as configurações através da função `get_db_config_maloka()`
- Salva as configurações no XCom para uso em outras DAGs

### Sistema de Fallback

A função `get_db_config()` no módulo `database.py`:

- Primeiro tenta carregar as configurações diretamente do módulo `airflow_variables.py`
- Se falhar, tenta buscar do XCom da DAG `dag_load_variables` 
- Se ainda assim falhar, tenta as variáveis de ambiente
- Mantém um cache em memória para evitar buscas repetidas

## Como Usar

### Executando a DAG de Variáveis

Antes de executar qualquer DAG que dependa das configurações de banco de dados, certifique-se de que a DAG `dag_load_variables` já foi executada pelo menos uma vez:

1. No Airflow UI, vá para a página DAGs
2. Encontre a DAG `dag_load_variables`
3. Execute-a manualmente (trigger) se necessário

### Definindo Variáveis no Airflow

Certifique-se de que as seguintes variáveis estejam configuradas no Airflow:

- `DB_MALOKA_HOST`: Host do banco de dados
- `DB_MALOKA_PORT`: Porta do banco de dados
- `DB_MALOKA_USER`: Usuário do banco de dados
- `DB_MALOKA_PASS`: Senha do banco de dados

### Desenvolvimento Local

Para desenvolvimento local, defina as variáveis de ambiente no arquivo `.env`:

```
DB_HOST=hostname
DB_PORT=5432
DB_USER=username
DB_PASS=password
```

## Solução de Problemas

Se as DAGs não conseguirem acessar as configurações de banco de dados:

1. Verifique se a DAG `dag_load_variables` foi executada com sucesso
2. Confirme que as variáveis estão configuradas corretamente no Airflow
3. Verifique os logs da DAG `dag_load_variables` para identificar problemas

## Manutenção

Se for necessário adicionar novas configurações:

1. Adicione-as à função `get_db_config_maloka()` no arquivo `airflow_variables.py`
2. Execute a DAG `dag_load_variables` para atualizar os valores no XCom
