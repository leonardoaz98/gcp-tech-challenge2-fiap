"""
Configuracao central do projeto.

Le as variaveis de ambiente do .env uma unica vez e expoe como constantes,
evitando que cada script repita load_dotenv() e os.getenv().
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Raiz do projeto ---
ROOT_DIR = Path(__file__).resolve().parent.parent

# --- Google Cloud ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "silver")
BQ_DATASET_GOLD = os.getenv("BQ_DATASET_GOLD", "gold")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "indicador-updates")
GCP_REGION = os.getenv("GCP_REGION", "southamerica-east1")

# --- Base dos Dados ---
BD_DATASET = "br_inep_avaliacao_alfabetizacao"

# --- Caminhos locais ---
DATA_DIR = ROOT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"

# --- Dominio ---
ANOS_META = [2024, 2025, 2026, 2027, 2028, 2029, 2030]

# Tabela -> colunas de particionamento na Bronze (lista vazia = sem particao)
TABELAS_BRONZE = {
    "uf": ["ano"],
    "municipio": ["ano"],
    "alunos": ["ano"],
    "meta_alfabetizacao_brasil": [],
    "meta_alfabetizacao_uf": [],
    "meta_alfabetizacao_municipio": [],
    "dicionario": [],
}

# Tabelas sem coluna 'ano' real na origem — foram replicadas por particao
# na ingestao e devem ser lidas de um unico arquivo
TABELAS_SEM_ANO = {
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
}


def validar_config() -> None:
    """Falha cedo se alguma variavel obrigatoria estiver ausente."""
    obrigatorias = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GCS_BUCKET": GCS_BUCKET,
    }
    faltando = [k for k, v in obrigatorias.items() if not v]
    if faltando:
        raise EnvironmentError(
            f"Variaveis ausentes no .env: {', '.join(faltando)}. "
            f"Copie .env.example para .env e preencha."
        )
