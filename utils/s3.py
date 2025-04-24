# utils/s3.py

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Client:
    def __init__(self, conn_id: str = None, bucket: str = None):
        from config.settings import AWS_CONN_ID, BUCKET_NAME
        self.conn_id = conn_id or AWS_CONN_ID
        self.bucket  = bucket  or BUCKET_NAME
        self.hook    = S3Hook(aws_conn_id=self.conn_id)
        # boto3 client
        self.client  = self.hook.get_conn()

    def upload_bytes(self, data: bytes, key: str):
        """Faz upload de um blob em memória para s3://{bucket}/{key}."""
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def download_bytes(self, key: str) -> bytes:
        """Baixa um blob de s3://{bucket}/{key} e retorna os bytes."""
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()
