"""
Ingestao Batch - Camada Bronze
Baixa as tabelas do dataset br_inep_avaliacao_alfabetizacao da Base dos Dados
e grava em Parquet particionado no Cloud Storage, sem transformacoes.
"""

import os
import logging
from datetime import datetime, timezone

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCS_BUCKET")
DATASET = "br_inep_avaliacao_alfabetizacao"
LOCAL_DIR = "data/bronze"

# Tabela -> colunas de particionamento (vazio = sem particao)
TABELAS = {
    "uf": ["ano"],
    "municipio": [],
    "alunos": ["ano"],
    "meta_alfabetizacao_brasil": ["ano"],
    "meta_alfabetizacao_uf": ["ano"],
    "meta_alfabetizacao_municipio": ["ano"],
    "dicionario": [],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bronze")


def extrair(tabela: str) -> pd.DataFrame:
    """Le a tabela completa da Base dos Dados via BigQuery."""
    query = f"SELECT * FROM `basedosdados.{DATASET}.{tabela}`"
    log.info(f"[{tabela}] extraindo...")
    df = bd.read_sql(query=query, billing_project_id=PROJECT)
    log.info(f"[{tabela}] {df.shape[0]} linhas x {df.shape[1]} colunas")
    return df


def gravar_local(df: pd.DataFrame, tabela: str, particoes: list) -> str:
    """Grava em Parquet, particionado quando aplicavel."""
    df["_ingestao_timestamp"] = datetime.now(timezone.utc)
    df["_fonte"] = f"basedosdados.{DATASET}.{tabela}"

    destino = f"{LOCAL_DIR}/{tabela}"
    os.makedirs(destino, exist_ok=True)

    particoes_validas = [c for c in particoes if c in df.columns]

    if particoes_validas:
        df.to_parquet(
            destino,
            partition_cols=particoes_validas,
            index=False,
            compression="snappy",
        )
        log.info(f"[{tabela}] particionado por {particoes_validas}")
    else:
        df.to_parquet(
            f"{destino}/{tabela}.parquet",
            index=False,
            compression="snappy",
        )
        log.info(f"[{tabela}] gravado sem particao")

    return destino


def enviar_bucket(caminho_local: str, tabela: str) -> int:
    """Sobe os arquivos locais para gs://BUCKET/bronze/<tabela>/."""
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(BUCKET)
    enviados = 0

    for raiz, _, arquivos in os.walk(caminho_local):
        for arquivo in arquivos:
            local = os.path.join(raiz, arquivo)
            relativo = os.path.relpath(local, caminho_local)
            blob_path = f"bronze/{tabela}/{relativo}"
            bucket.blob(blob_path).upload_from_filename(local)
            enviados += 1

    log.info(f"[{tabela}] {enviados} arquivo(s) enviado(s) ao bucket")
    return enviados


def main():
    log.info(f"Iniciando ingestao Bronze | projeto={PROJECT} | bucket={BUCKET}")
    sucesso, falha = [], []

    for tabela, particoes in TABELAS.items():
        try:
            df = extrair(tabela)
            caminho = gravar_local(df, tabela, particoes)
            enviar_bucket(caminho, tabela)
            sucesso.append(tabela)
        except Exception as erro:
            log.error(f"[{tabela}] FALHOU: {erro}")
            falha.append(tabela)

    log.info(f"Concluido | sucesso={len(sucesso)} | falha={len(falha)}")
    if falha:
        log.warning(f"Tabelas com falha: {falha}")


if __name__ == "__main__":
    main()
