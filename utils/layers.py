# utils/layers.py

import pandas as pd
from io import BytesIO
from datetime import datetime
from config.settings import LAYER_PATHS
from .s3 import S3Client

class BaseLayer:
    def __init__(self, s3_client: S3Client = None):
        self.s3 = s3_client or S3Client()

class TransientLayer(BaseLayer):
    def write(self, df: pd.DataFrame, filename: str) -> str:
        """
        Grava CSV em memória e faz upload para S3 em
        LAYER_PATHS['transient'] + filename
        Retorna a S3 key (prefix+filename).
        """
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        key = LAYER_PATHS["transient"] + filename
        self.s3.upload_bytes(buf.getvalue(), key)
        return key

class BronzeLayer(BaseLayer):
    def process(self, transient_key: str) -> str:
        """
        Lê CSV do S3 (transient_key), adiciona DATA_EXTRACAO,
        converte para Parquet em memória e envia para
        LAYER_PATHS['bronze'] com extensão .parquet.
        Retorna a S3 key do parquet.
        """
        data = self.s3.download_bytes(transient_key)
        df = pd.read_csv(BytesIO(data))

        df["DATA_EXTRACAO"] = datetime.utcnow().isoformat()

        out_buf = BytesIO()
        df.to_parquet(out_buf, index=False)
        out_buf.seek(0)

        bronze_key = (
            transient_key
            .replace("transient/", "bronze/")
            .replace(".csv", ".parquet")
        )
        self.s3.upload_bytes(out_buf.getvalue(), bronze_key)
        return bronze_key

class SilverLayer(BaseLayer):
    def process(self, bronze_key: str, bk_cols: list) -> str:
        """
        Lê Parquet do S3 (bronze_key), gera coluna BK_COLUMNS a partir
        de bk_cols, aplica dropna(), regrava em Parquet e envia para
        LAYER_PATHS['silver'].
        Retorna a S3 key do silver.
        """
        data = self.s3.download_bytes(bronze_key)
        df   = pd.read_parquet(BytesIO(data))

        df["BK_COLUMNS"] = df[bk_cols].astype(str).agg("_".join, axis=1)
        df = df.dropna(how="any")

        out_buf = BytesIO()
        df.to_parquet(out_buf, index=False)
        out_buf.seek(0)

        silver_key = bronze_key.replace("/bronze/", "/silver/")
        self.s3.upload_bytes(out_buf.getvalue(), silver_key)
        return silver_key
