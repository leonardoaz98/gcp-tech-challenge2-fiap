"""
Promocao da zona de streaming: Bronze -> Silver.

Le os Parquet gravados pelo consumer no bucket, deduplica por `id_evento`,
aplica a mesma padronizacao do caminho batch e materializa
`silver.evento_indicador`.

A deduplicacao aqui e o que converte a garantia at-least-once do Pub/Sub
em semantica efetivamente exactly-once na camada analitica: o consumer
pode reentregar a mesma mensagem apos uma falha, e o `id_evento` resolve.

Escrita em modo append com janela: apenas eventos ainda nao presentes na
Silver sao inseridos, entao a execucao e idempotente.
"""

import io

import pandas as pd
import pandas_gbq
from google.cloud import storage

from config.logger import get_logger
from config.settings import (
    BQ_DATASET_SILVER,
    BRONZE_STREAMING_PREFIX,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    SILVER_TABELA_EVENTOS,
    validar_config,
)
from quality.validations import relatorio_consolidado, validar_tabela

log = get_logger("streaming")

DESTINO = f"{BQ_DATASET_SILVER}.{SILVER_TABELA_EVENTOS}"


def ler_bronze_streaming() -> pd.DataFrame:
    """Concatena todos os micro-batches da zona de streaming."""
    client = storage.Client(project=GCP_PROJECT_ID)
    blobs = [
        b for b in client.list_blobs(GCS_BUCKET, prefix=BRONZE_STREAMING_PREFIX)
        if b.name.endswith(".parquet")
    ]

    if not blobs:
        log.warning("[streaming] nenhum evento na zona de streaming da Bronze")
        return pd.DataFrame()

    partes = [
        pd.read_parquet(io.BytesIO(blob.download_as_bytes())) for blob in blobs
    ]
    df = pd.concat(partes, ignore_index=True)
    log.info(f"[streaming] {len(df)} evento(s) lidos de {len(blobs)} micro-batch(es)")
    return df


def padronizar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica as mesmas regras de tipagem do caminho batch."""
    df = df.copy()
    df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str.zfill(7)
    df["sigla_uf"] = df["sigla_uf"].astype(str).str.strip().str.upper()
    df["ano_referencia"] = pd.to_numeric(
        df["ano_referencia"], errors="coerce"
    ).astype("Int64")
    df["timestamp_evento"] = pd.to_datetime(df["timestamp_evento"], utc=True)
    return df.drop(columns=["_fonte"], errors="ignore")


def deduplicar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantem a ultima ocorrencia de cada id_evento.

    Converte o at-least-once do broker em exactly-once na Silver.
    """
    antes = len(df)
    df = (
        df.sort_values("_ingestao_timestamp")
        .drop_duplicates(subset=["id_evento"], keep="last")
        .reset_index(drop=True)
    )
    if len(df) < antes:
        log.info(f"[streaming] {antes - len(df)} reentrega(s) deduplicada(s)")
    return df


def filtrar_novos(df: pd.DataFrame) -> pd.DataFrame:
    """Descarta eventos ja materializados, tornando a carga idempotente."""
    try:
        existentes = pandas_gbq.read_gbq(
            f"SELECT id_evento FROM `{GCP_PROJECT_ID}.{DESTINO}`",
            project_id=GCP_PROJECT_ID,
            progress_bar_type=None,
        )["id_evento"]
    except Exception:
        log.info("[streaming] tabela ainda nao existe — carga inicial")
        return df

    novos = df[~df["id_evento"].isin(set(existentes))]
    log.info(f"[streaming] {len(novos)} evento(s) novos de {len(df)} lidos")
    return novos


def main() -> None:
    validar_config()
    log.info("=== Promovendo streaming Bronze -> Silver ===")

    bruto = ler_bronze_streaming()
    if bruto.empty:
        log.info("=== Nada a promover ===")
        return

    eventos = filtrar_novos(deduplicar(padronizar(bruto)))
    if eventos.empty:
        log.info("=== Silver ja esta atualizada ===")
        return

    resultado = validar_tabela(eventos, SILVER_TABELA_EVENTOS, ["id_evento"])

    pandas_gbq.to_gbq(
        eventos,
        destination_table=DESTINO,
        project_id=GCP_PROJECT_ID,
        if_exists="append",
        progress_bar=False,
    )
    log.info(f"[silver] {DESTINO} atualizado (+{len(eventos)} linhas)")

    print(relatorio_consolidado([resultado]).to_string(index=False))
    log.info("=== Promocao concluida ===")


if __name__ == "__main__":
    main()
