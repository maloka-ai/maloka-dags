# config/settings.py

# bucket e conexão de S3
BUCKET_NAME = "malokaai"
AWS_CONN_ID  = "s3-conn-add"

# prefixo “root” para este conjunto de DAGs (neste caso: add/)
PREFIX = "add/"

LAYER_PATHS = {
    "transient": PREFIX + "transient/",
    "bronze":    PREFIX + "bronze/",
    "silver":    PREFIX + "silver/",
    # future: "gold": PREFIX + "gold/",
}
