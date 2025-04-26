from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from config.settings import BUCKET_NAME

class S3Client:
    def __init__(self, aws_conn_id: str, bucket: str = None):
        self.conn_id = aws_conn_id
        self.bucket  = bucket or BUCKET_NAME
        self.hook    = S3Hook(aws_conn_id=self.conn_id)
        self.client  = self.hook.get_conn()

    def upload_bytes(self, data: bytes, key: str):
        """Faz upload de um blob em memória para s3://{bucket}/{key}."""
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def download_bytes(self, key: str) -> bytes:
        """Baixa um blob de s3://{bucket}/{key} e retorna os bytes."""
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()