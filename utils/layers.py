import pandas as pd
from io import BytesIO
from datetime import datetime
from .s3 import S3Client

class BaseLayer:
    def __init__(self, prefix: str, aws_conn_id: str):
        self.prefix = prefix  # ex: "add/"
        self.s3     = S3Client(aws_conn_id)

class TransientLayer(BaseLayer):
    def write(self, df: pd.DataFrame, filename: str) -> str:
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        key = f"{self.prefix}transient/{filename}"
        self.s3.upload_bytes(buf.getvalue(), key)
        return key

class BronzeLayer(BaseLayer):
    def process(self, transient_key: str) -> str:
        data = self.s3.download_bytes(transient_key)
        df   = pd.read_csv(BytesIO(data))
        df["DATA_EXTRACAO"] = datetime.utcnow().isoformat()

        out_buf = BytesIO()
        df.to_parquet(out_buf, index=False)
        out_buf.seek(0)

        bronze_key = transient_key.replace("/transient/", "/bronze/")\
                                  .replace(".csv", ".parquet")
        self.s3.upload_bytes(out_buf.getvalue(), bronze_key)
        return bronze_key

class SilverLayer(BaseLayer):
    def process(self, bronze_key: str, bk_cols: list) -> str:
        data = self.s3.download_bytes(bronze_key)
        df   = pd.read_parquet(BytesIO(data))
        df["BK_COLUMNS"] = df[bk_cols].astype(str).agg("_".join, axis=1)
        df   = df.dropna(how="any")

        out_buf = BytesIO()
        df.to_parquet(out_buf, index=False)
        out_buf.seek(0)

        silver_key = bronze_key.replace("/bronze/", "/silver/")
        self.s3.upload_bytes(out_buf.getvalue(), silver_key)
        return silver_key