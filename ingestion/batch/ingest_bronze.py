"""
Ingestao Batch - Camada Bronze

Baixa as tabelas do dataset br_inep_avaliacao_alfabetizacao da Base dos
Dados e grava em Parquet no Cloud Storage, sem transformacoes.
"""

import os
from datetime import datetime, timezone

import basedosdados as bd
import pandas as pd
from google.cloud import storage

from config.logger import get_logger
from config.settings import (
    BD_DATASET,
    BRONZE_DIR,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    TABELAS_BRONZE,
    validar_config,
)

log = get_logger("bronze")


def extrair(tabela: str) -> pd.DataFrame:
    """Le a tabela completa da Base dos Dados via BigQuery."""
    query = f"SELECT * FROM `basedosdados.{BD_DATASET}.{tabela}`"
    log.info(f"[{tabela}] extraindo...")
    df = bd.read_sql(query=query, billing_project_id=GCP_PROJECT_ID)
    log.info(f"[{tabela}] {df.shape[0]} linhas x {df.shape[1]} colunas")
    return df


def gravar_local(df: pd.DataFrame, tabela: str, particoes: list) -> str:
    """
    Grava em Parquet, particionado quando aplicavel.

    Adiciona metadados de rastreabilidade exigidos pela governanca.
    """
    df = df.copy()
    df["_ingestao_timestamp"] = datetime.now(timezone.utc)
    df["_fonte"] = f"basedosdados.{BD_DATASET}.{tabela}"

    destino = BRONZE_DIR / tabela
    destino.mkdir(parents=True, exist_ok=True)

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
            destino / f"{tabela}.parquet",
            index=False,
            compression="snappy",
        )
        log.info(f"[{tabela}] gravado sem particao")

    return str(destino)


def enviar_bucket(caminho_local: str, tabela: str) -> int:
    """Sobe os arquivos locais para gs://BUCKET/bronze/<tabela>/."""
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    enviados = 0

    for raiz, _, arquivos in os.walk(caminho_local):
        for arquivo in arquivos:
            local = os.path.join(raiz, arquivo)
            relativo = os.path.relpath(local, caminho_local)
            bucket.blob(f"bronze/{tabela}/{relativo}").upload_from_filename(local)
            enviados += 1

    log.info(f"[{tabela}] {enviados} arquivo(s) enviado(s) ao bucket")
    return enviados


def main() -> None:
    validar_config()
    log.info(f"Iniciando ingestao Bronze | projeto={GCP_PROJECT_ID} | bucket={GCS_BUCKET}")

    sucesso, falha = [], []

    for tabela, particoes in TABELAS_BRONZE.items():
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
