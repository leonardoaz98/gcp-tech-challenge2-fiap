"""
Consumer - ingestao dos eventos na zona de streaming da Bronze.

Consome do Pub/Sub em micro-batches e grava Parquet em
`gs://<bucket>/bronze/streaming/dt=YYYY-MM-DD/`.

Por que micro-batch e nao um arquivo por evento: object storage cobra por
operacao, e milhares de objetos minusculos degradam a leitura analitica -
o classico small files problem. O buffer descarrega quando atinge
MAX_EVENTOS ou MAX_SEGUNDOS, o que vier primeiro, equilibrando latencia e
custo.

O ack so acontece depois da gravacao confirmada no bucket. Se o processo
morrer no meio, o Pub/Sub reentrega a mensagem - garantia at-least-once.
A deduplicacao por `id_evento` acontece na promocao para a Silver.

Uso:
    python -m ingestion.streaming.consumer --duracao 120
"""

import argparse
import io
import json
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import pubsub_v1, storage

from config.logger import get_logger
from config.settings import (
    BRONZE_STREAMING_PREFIX,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    PUBSUB_SUBSCRIPTION,
    validar_config,
)

log = get_logger("consumer")

MAX_EVENTOS = 100
MAX_SEGUNDOS = 30

# Metricas de observabilidade do pipeline streaming
METRICAS = {
    "recebidos": 0,
    "gravados": 0,
    "invalidos": 0,
    "lotes": 0,
    "latencia_total": 0.0,
}

CAMPOS_OBRIGATORIOS = {"id_evento", "id_municipio", "ano_referencia", "timestamp_evento"}


def validar_evento(evento: dict) -> bool:
    """Rejeita evento sem os campos minimos para ser rastreavel."""
    faltando = CAMPOS_OBRIGATORIOS - evento.keys()
    if faltando:
        log.warning(f"[consumer] evento descartado, campos ausentes: {faltando}")
        return False
    return True


def gravar_lote(eventos: list, bucket) -> None:
    """Materializa o micro-batch como um unico Parquet no bucket."""
    if not eventos:
        return

    df = pd.DataFrame(eventos)
    df["_ingestao_timestamp"] = datetime.now(timezone.utc)
    df["_fonte"] = f"pubsub://{PUBSUB_SUBSCRIPTION}"

    agora = datetime.now(timezone.utc)
    particao = agora.strftime("dt=%Y-%m-%d")
    nome = f"eventos_{agora.strftime('%Y%m%dT%H%M%S%f')}.parquet"
    caminho = f"{BRONZE_STREAMING_PREFIX}/{particao}/{nome}"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)

    bucket.blob(caminho).upload_from_file(
        buffer, content_type="application/octet-stream"
    )

    METRICAS["gravados"] += len(df)
    METRICAS["lotes"] += 1
    log.info(f"[consumer] lote {METRICAS['lotes']}: {len(df)} evento(s) -> gs://{GCS_BUCKET}/{caminho}")


def consumir(duracao: int) -> None:
    subscriber = pubsub_v1.SubscriberClient()
    caminho_sub = subscriber.subscription_path(GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION)

    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)

    buffer: list = []
    ack_ids: list = []
    ultimo_flush = time.time()
    fim = time.time() + duracao

    log.info(f"[consumer] escutando {PUBSUB_SUBSCRIPTION} por {duracao}s")

    while time.time() < fim:
        resposta = subscriber.pull(
            request={"subscription": caminho_sub, "max_messages": MAX_EVENTOS},
            timeout=10,
        )

        for recebida in resposta.received_messages:
            METRICAS["recebidos"] += 1
            try:
                evento = json.loads(recebida.message.data.decode("utf-8"))
            except json.JSONDecodeError:
                METRICAS["invalidos"] += 1
                ack_ids.append(recebida.ack_id)  # payload corrompido nao se recupera
                continue

            if not validar_evento(evento):
                METRICAS["invalidos"] += 1
                ack_ids.append(recebida.ack_id)
                continue

            # Latencia fim a fim: da emissao do evento ate o consumo
            emissao = datetime.fromisoformat(evento["timestamp_evento"])
            METRICAS["latencia_total"] += (
                datetime.now(timezone.utc) - emissao
            ).total_seconds()

            buffer.append(evento)
            ack_ids.append(recebida.ack_id)

        estourou_tamanho = len(buffer) >= MAX_EVENTOS
        estourou_tempo = (time.time() - ultimo_flush) >= MAX_SEGUNDOS

        if buffer and (estourou_tamanho or estourou_tempo):
            gravar_lote(buffer, bucket)
            # Ack somente apos a gravacao confirmada: at-least-once
            subscriber.acknowledge(
                request={"subscription": caminho_sub, "ack_ids": ack_ids}
            )
            buffer, ack_ids = [], []
            ultimo_flush = time.time()

    # Descarga final do que sobrou no buffer
    if buffer:
        gravar_lote(buffer, bucket)
        subscriber.acknowledge(
            request={"subscription": caminho_sub, "ack_ids": ack_ids}
        )

    subscriber.close()
    relatorio()


def relatorio() -> None:
    """Resumo de observabilidade da janela de consumo."""
    processados = METRICAS["gravados"]
    latencia = (
        METRICAS["latencia_total"] / processados if processados else 0.0
    )

    log.info("=== Relatorio da ingestao streaming ===")
    log.info(f"  eventos recebidos : {METRICAS['recebidos']}")
    log.info(f"  eventos gravados  : {METRICAS['gravados']}")
    log.info(f"  eventos invalidos : {METRICAS['invalidos']}")
    log.info(f"  micro-batches     : {METRICAS['lotes']}")
    log.info(f"  latencia media    : {latencia:.2f}s")

    if METRICAS["invalidos"]:
        log.warning(
            f"[alerta] {METRICAS['invalidos']} evento(s) descartados por "
            "falha de schema"
        )


def main() -> None:
    validar_config()
    parser = argparse.ArgumentParser(description="Consumer da ingestao streaming")
    parser.add_argument(
        "--duracao", type=int, default=120, help="segundos de escuta"
    )
    args = parser.parse_args()

    consumir(args.duracao)


if __name__ == "__main__":
    main()
