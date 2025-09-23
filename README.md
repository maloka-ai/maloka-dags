# Maloka Airflow Pipelines

Este repositório contém as DAGs e utilitários para construção de pipelines de ingestão e transformação de dados no Apache Airflow, usando uma arquitetura em camadas (transient → bronze → silver → gold) para múltiplas empresas.

---

## 📁 Estrutura de Pastas

```text
airflow/
├── config/
│   ├── settings.py            # Configuração global (nome do bucket)
│   └── airflow_variables.py   # Variáveis do Airflow e utilitários de configuração
├── utils/
│   ├── s3.py                  # Cliente S3 com conexão e upload/download
│   ├── layers.py              # Classes de camada (Transient, Bronze, Silver)
│   ├── database.py            # Cliente de banco de dados para conexões e consultas
│   └── triggers.py            # Utilitários para triggers de validação de banco de dados
└── dags/
    ├── add/                   # Pipeline "add" (exemplo); pode duplicar para outras empresas
    │   ├── dag_add_pipeline.py       # DAG única com TaskGroup por camada
    │   └── dag_add_gold_postgres.py  # Exemplo de DAG para camada gold e carga no Postgres
    ├── modelagens/
    │   ├── add_dag.py               # DAG de modelagens para o cliente ADD com validação de banco
    │   └── analytics/               # Módulos de análises e modelagens
    └── template_dag_com_validacao.py # Template de DAG com validação de banco

├── requirements.txt           # Dependências Python
└── README.md                  # Este arquivo
```

---

## ⚙️ Pré-Requisitos

1. **Sistema Operacional**: Ubuntu 24.04 ou similar
2. **Python**: >= 3.12

---

## 🛠️ Recursos Principais

### Validação de Banco de Dados para Execução de DAGs

O sistema implementa um mecanismo de validação que verifica se o banco de dados do cliente está pronto para atualização antes de executar as DAGs. Isso é útil para evitar processamentos desnecessários quando o banco de dados ainda não foi atualizado.

#### Como funciona:

1. Antes de executar as tarefas principais, a DAG verifica na tabela `configuracao.log_processamento_dados` se existem dados importados que ainda não foram processados pelas modelagens
2. A verificação procura o registro mais recente e valida se a coluna `data_execucao_modelagem` é `None`
3. Se o banco não estiver pronto, a DAG aguarda e tenta novamente após um intervalo configurável (padrão: 15 minutos)
4. Ao finalizar o processamento com sucesso, a DAG registra a data de execução da modelagem na mesma tabela

#### Estrutura do Banco de Dados:

A tabela `configuracao.log_processamento_dados` deve ter a seguinte estrutura:

```sql
CREATE TABLE configuracao.log_processamento_dados (
    id_log SERIAL PRIMARY KEY,
    cliente_id VARCHAR(50) NOT NULL,
    data_importacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_execucao_modelagem TIMESTAMP,
    mensagem TEXT
);
```

#### Como usar em uma DAG:

1. Importe as funções necessárias:
```python
from utils.triggers import (
    criar_deferrable_task_atualizacao_banco, 
    registrar_sucesso_atualizacao,
    registrar_falha_atualizacao
)
```

2. Crie uma task de validação:
```python
verificar_atualizacao = criar_deferrable_task_atualizacao_banco(
    task_id='verificar_atualizacao_banco',
    conn_id='sua_conexao_db',
    cliente_id='id_do_cliente',
    intervalo_verificacao_minutos=30
)
```

3. Defina as tarefas para registro de sucesso e falha:
```python
# Registro de sucesso
registrar_sucesso = PythonOperator(
    task_id='registrar_sucesso',
    python_callable=registrar_sucesso_atualizacao,
    op_kwargs={'conn_id': 'sua_conexao_db', 'cliente_id': 'id_do_cliente'}
)

# Registro de falha
registrar_falha = PythonOperator(
    task_id='registrar_falha',
    python_callable=registrar_falha_atualizacao,
    op_kwargs={'conn_id': 'sua_conexao_db', 'cliente_id': 'id_do_cliente'},
    trigger_rule='one_failed'
)
```

4. Veja um exemplo completo em `dags/template_dag_com_validacao.py`
3. **Apache Airflow**: >= 2.7.1
4. **Banco de Metadados**: PostgreSQL (recomendado) ou SQLite (apenas dev)
5. **Credenciais AWS**: Usuário IAM com permissão S3
6. **GitHub Actions**: configurado no CI/CD

---

## 🚀 Instalação e Setup

### 1. Clonar o repositório

```bash
git clone <URL_DO_REPO> ~/airflow/dags
cd ~/airflow\```

### 2. Configurar o virtualenv

```bash
python3 -m venv airflow-venv
source airflow-venv/bin/activate
pip install --upgrade pip
pip install -r dags/requirements.txt
```

### 3. Configurar o Metastore

> **Recomendado:** PostgreSQL + LocalExecutor

```bash
sudo apt update && sudo apt install -y postgresql libpq-dev
sudo -u postgres createuser airflow --createdb --no-superuser
sudo -u postgres psql -c "ALTER USER airflow WITH PASSWORD 'SUA_SENHA';"
sudo -u postgres createdb airflow --owner airflow

# Em airflow.cfg:
# sql_alchemy_conn = postgresql+psycopg2://airflow:SUA_SENHA@localhost:5432/airflow
# executor = LocalExecutor
# load_examples = False

export AIRFLOW_HOME=~/airflow
airflow db init
```

> **Alternativa (dev):** SQLite (paliativo)
```ini
# em airflow.cfg:
# executor = SequentialExecutor
# [scheduler]
# parsing_processes = 1
# max_threads = 1
```

### 4. Criar Usuário Admin (RBAC)

```bash
airflow users create \
  --username admin \
  --firstname Maloka \
  --lastname Admin \
  --role Admin \
  --email admin@maloka.ai \
  --password youpass
```

### 5. Conexão S3 no Airflow

- Acesse **Admin → Connections** na UI
- Adicione conexão:
  - Conn ID: `s3-conn-add`
  - Conn Type: **Amazon Web Services**
  - Login: `<AWS_ACCESS_KEY_ID>`
  - Password: `<AWS_SECRET_ACCESS_KEY>`
  - Extra JSON:
    ```json
    {"region_name":"us-east-1"}
    ```

---

## 📜 Configuração Global

```python
# config/settings.py
BUCKET_NAME = "malokaai"
```

---

## 🛠️ Pipelines

### DAG: `add_pipeline`

- **Pipeline completo** com TaskGroup para transient, bronze e silver
- **Conexões**: `s3-conn-add`
- **Prefixo**: `add/`

Fluxo:
1. **Transient**: gera CSV e faz upload em `s3://malokaai/add/transient/`
2. **Bronze**: lê CSV, adiciona `DATA_EXTRACAO`, escreve Parquet em `s3://malokaai/add/bronze/`
3. **Silver**: lê Parquet, gera `BK_COLUMNS`, higieniza e escreve Parquet em `s3://malokaai/add/silver/`

### DAG: `add_gold_postgres`

- **Camada Gold**: lê Parquet da silver, grava Parquet em `s3://malokaai/add/gold/`
- **Carga Postgres**: insere/atualiza dados na tabela `dados_gold` do banco `add`
- **Conexões**: `s3-conn-add`, `ADD-POSTGRES-POCDASHBOARD`

---

## 📥 Execução Manual

```bash
export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080 --daemon
airflow scheduler --daemon
```
Acesse `http://<IP_OU_HOST>/airflow` e faça login com `admin ou seu usuario`.

Para disparar:
```bash
airflow dags trigger add_pipeline
airflow dags trigger add_gold_postgres
```

---

## ⚙️ CI/CD (GitHub Actions)

```yaml
name: Deploy Airflow DAGs to EC2
on: [push]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy via SSH
        uses: appleboy/ssh-action@master
        with:
          host: ${{ secrets.HOST_DNS }}
          username: ${{ secrets.USERNAME }}
          key: ${{ secrets.EC2_SSH_KEY }}
          script: |
            cd /home/ubuntu/airflow/dags
            git fetch origin main && git reset --hard origin/main
            cd /home/ubuntu
            source airflow-venv/bin/activate
            pip install --upgrade pip
            pip install -r airflow/dags/requirements.txt
            pkill -f "airflow webserver"; airflow webserver --port 8080 --daemon
            pkill -f "airflow scheduler"; airflow scheduler --daemon
```

---

## 🎯 Boas Práticas

- **Modularize** seu código em `utils/` e `config/` para reuso.
- **Use TaskGroup** para organizar visualmente camadas na DAG.
- **Evite download local** se seus arquivos couberem em memória.
- **Use Postgres + LocalExecutor** em produção.
- **Gerencie processos** via systemd para alta disponibilidade.
- **Versione** suas DAGs e requirements via Git.

---

## 🔍 Consultas no PostgreSQL teste

Para verificar os dados inseridos na tabela **dados_gold**, siga estes passos:

1. Conecte-se ao banco **add** no psql:

   ```bash
   psql -h <HOST> -U <USUÁRIO> -d add
   ```

2. (Opcional) Liste as colunas da tabela **dados_gold**:

   ```sql
   \d dados_gold
   ```

3. Execute sua consulta para visualizar todos os registros:

   ```sql
   SELECT *
   FROM dados_gold
   ORDER BY id;
   ```

4. Para sair do psql:

   ```sql
   \q
   ```

*Maloka Data Engineering*

