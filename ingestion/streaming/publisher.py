"""
Publisher - simulacao de eventos de atualizacao do indicador.

Reproduz o cenario real em que estados e municipios enviam correcoes e
novas medicoes fora do ciclo anual do Saeb. Os municipios sorteados sao
lidos da propria Gold, entao os eventos referenciam chaves que existem de
fato - um gerador com IDs sinteticos produziria 100% de orfas na
validacao de integridade e mascararia o comportamento real do pipeline.

Uso:
    python -m ingestion.streaming.publisher --eventos 200 --intervalo 0.1
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import pandas_gbq
from google.cloud import pubsub_v1

from config.logger import get_logger
from config.settings import (
    BQ_DATASET_GOLD,
    GCP_PROJECT_ID,
    PUBSUB_TOPIC,
    validar_config,
)

log = get_logger("publisher")

TIPOS_EVENTO = [
    "atualizacao_indicador",
    "nova_medicao",
    "revisao_meta",
]

ORIGENS = ["secretaria_municipal", "secretaria_estadual", "inep"]


def amostrar_municipios(limite: int = 500) -> list:
    """Sorteia municipios reais da dimensao para lastrear os eventos."""
    sql = f"""
        SELECT id_municipio, sigla_uf
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.dim_municipio`
        ORDER BY RAND()
        LIMIT {limite}
    """
    df = pandas_gbq.read_gbq(sql, project_id=GCP_PROJECT_ID, progress_bar_type=None)
    log.info(f"[publisher] {len(df)} municipios carregados para amostragem")
    return df.to_dict("records")


def gerar_evento(municipios: list) -> dict:
    """Monta um evento com esquema estavel e chave de deduplicacao."""
    municipio = random.choice(municipios)

    return {
        "id_evento": str(uuid.uuid4()),
        "tipo_evento": random.choice(TIPOS_EVENTO),
        "origem": random.choice(ORIGENS),
        "id_municipio": municipio["id_municipio"],
        "sigla_uf": municipio["sigla_uf"],
        "ano_referencia": random.choice([2023, 2024, 2025]),
        "taxa_alfabetizacao": round(random.uniform(25.0, 98.0), 2),
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
    }


def publicar(quantidade: int, intervalo: float) -> None:
    publisher = pubsub_v1.PublisherClient()
    topico = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)
    municipios = amostrar_municipios()

    log.info(f"[publisher] publicando {quantidade} evento(s) em {PUBSUB_TOPIC}")
    futuros = []

    for i in range(1, quantidade + 1):
        evento = gerar_evento(municipios)
        corpo = json.dumps(evento).encode("utf-8")

        # Atributos viajam fora do payload e permitem filtro na
        # subscription sem desserializar a mensagem.
        futuro = publisher.publish(
            topico,
            corpo,
            tipo_evento=evento["tipo_evento"],
            sigla_uf=evento["sigla_uf"],
        )
        futuros.append(futuro)

        if i % 50 == 0:
            log.info(f"[publisher] {i}/{quantidade} publicados")

        time.sleep(intervalo)

    for futuro in futuros:
        futuro.result()

    log.info(f"[publisher] {quantidade} evento(s) confirmados pelo broker")


def main() -> None:
    validar_config()
    parser = argparse.ArgumentParser(description="Simulador de eventos do indicador")
    parser.add_argument("--eventos", type=int, default=200, help="quantidade a publicar")
    parser.add_argument(
        "--intervalo", type=float, default=0.1, help="segundos entre eventos"
    )
    args = parser.parse_args()

    publicar(args.eventos, args.intervalo)


if __name__ == "__main__":
    main()
