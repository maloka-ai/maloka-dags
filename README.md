# Maloka Airflow Pipelines

Este repositório contém as DAGs e utilitários para construção de pipelines de ingestão e transformação de dados no Apache Airflow, usando uma arquitetura em camadas (transient → bronze → silver) para múltiplas empresas.

---

## 📁 Estrutura de Pastas

```text
airflow/
├── config/
│   └── settings.py          # Configuração global (nome do bucket)
├── utils/
│   ├── s3.py                # Cliente S3 com conexão e upload/download
│   └── layers.py            # Classes de camada (Transient, Bronze, Silver)
└── dags/
    └── add/                 # Pipeline "add" (exemplo); pode replicar para outras empresas
        └── dag_add_pipeline.py  # DAG única com TaskGroup por camada

├── requirements.txt         # Dependências Python
└── README.md                # Este arquivo
```

---

## ⚙️ Pré‑Requisitos

1. **Sistema Operacional**: Ubuntu 24.04 ou similar
2. **Python**: >= 3.12
3. **Apache Airflow**: >= 2.7.1
4. **PostgreSQL**: para metastore (recomendado) ou SQLite (apenas dev)
5. **AWS Credentials**: Usuário IAM com permissão S3
6. **GitHub Actions**: configurado no CI/CD

---

## 🚀 Instalação e Setup

### 1. Clonar o repositório

```bash
git clone <URL_DO_REPO> ~/airflow/dags
cd ~/airflow
```

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
# Instalar e configurar PostgreSQL
sudo apt update && sudo apt install -y postgresql libpq-dev
sudo -u postgres createuser airflow --createdb --no-superuser
sudo -u postgres psql -c "ALTER USER airflow WITH PASSWORD 'SUA_SENHA';"
sudo -u postgres createdb airflow --owner airflow

# Ajustar airflow.cfg em ~/airflow/airflow.cfg
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
- Crie conexão:
  - Conn ID: `s3-conn-add`
  - Conn Type: **Amazon Web Services**
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
- ID: `add_pipeline`
- Schedule: `@daily`
- Camadas:
  1. **transient**: gera CSV e faz upload em `s3://malokaai/add/transient/`
  2. **bronze**: lê CSV, adiciona `DATA_EXTRACAO`, converte para Parquet em `s3://malokaai/add/bronze/`
  3. **silver**: lê Parquet, gera `BK_COLUMNS`, limpa dados, grava em `s3://malokaai/add/silver/`

> **Observação**: para criar pipelines de outras empresas, duplique a DAG e ajuste:
> ```python
> AWS_CONN_ID    = "s3-conn-<empresa>"
> COMPANY_PREFIX = "<empresa>/"
> ```

---

## 📥 Execução Manual

**Iniciar Airflow** (dentro do venv):
```bash
export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080 --daemon
airflow scheduler --daemon
```
Acesse `http://<IP_OU_HOST>/airflow` e faça login com `SeuUSer`.

**Trigger DAG** no UI ou CLI:
```bash
airflow dags trigger add_pipeline
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
            # reiniciar Airflow
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

*Maloka Data Engineering*