"""
Provisionamento do Pub/Sub.

Cria topico e subscription de forma idempotente. Rodar uma vez antes do
publisher e do consumer.
"""

from google.api_core import exceptions
from google.cloud import pubsub_v1

from config.logger import get_logger
from config.settings import (
    GCP_PROJECT_ID,
    PUBSUB_SUBSCRIPTION,
    PUBSUB_TOPIC,
    validar_config,
)

log = get_logger("pubsub")

# Mensagem nao consumida expira em 7 dias; evita acumulo silencioso de
# backlog e o custo de armazenamento associado.
RETENCAO_SEGUNDOS = 7 * 24 * 60 * 60

# Tempo para o consumer confirmar o processamento antes do redelivery.
ACK_DEADLINE = 60


def criar_topico() -> str:
    publisher = pubsub_v1.PublisherClient()
    caminho = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)

    try:
        publisher.create_topic(request={"name": caminho})
        log.info(f"[topico] criado: {PUBSUB_TOPIC}")
    except exceptions.AlreadyExists:
        log.info(f"[topico] ja existe: {PUBSUB_TOPIC}")

    return caminho


def criar_subscription(topico: str) -> str:
    subscriber = pubsub_v1.SubscriberClient()
    caminho = subscriber.subscription_path(GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION)

    try:
        subscriber.create_subscription(
            request={
                "name": caminho,
                "topic": topico,
                "ack_deadline_seconds": ACK_DEADLINE,
                "message_retention_duration": {"seconds": RETENCAO_SEGUNDOS},
            }
        )
        log.info(f"[subscription] criada: {PUBSUB_SUBSCRIPTION}")
    except exceptions.AlreadyExists:
        log.info(f"[subscription] ja existe: {PUBSUB_SUBSCRIPTION}")

    subscriber.close()
    return caminho


def main() -> None:
    validar_config()
    log.info(f"Provisionando Pub/Sub | projeto={GCP_PROJECT_ID}")
    topico = criar_topico()
    criar_subscription(topico)
    log.info("Provisionamento concluido")


if __name__ == "__main__":
    main()
