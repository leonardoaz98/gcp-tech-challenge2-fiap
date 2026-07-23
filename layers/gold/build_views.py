"""Cria as views analiticas da camada Gold usadas pelo dashboard."""
import logging
import pandas_gbq
from google.cloud import bigquery
from config.settings import GCP_PROJECT_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("views")

VIEWS = {
    "vw_regiao_ano": """
        SELECT
          u.regiao,
          f.ano,
          COUNT(*) AS municipios,
          ROUND(AVG(f.taxa_realizada), 1) AS taxa_media,
          ROUND(AVG(f.meta), 1) AS meta_media,
          ROUND(AVG(f.gap_meta), 1) AS gap_medio,
          ROUND(100 * AVG(CAST(f.meta_atingida AS INT64)), 1) AS pct_atingiu
        FROM `{p}.gold.fato_alfabetizacao` f
        JOIN `{p}.gold.dim_municipio` m ON f.id_municipio = m.id_municipio
        JOIN `{p}.gold.dim_uf` u ON m.sigla_uf = u.sigla_uf
        WHERE f.gap_meta IS NOT NULL
        GROUP BY 1, 2
    """,
    "vw_uf_ano": """
        SELECT
          u.sigla_uf,
          u.regiao,
          f.ano,
          COUNT(*) AS municipios,
          ROUND(AVG(f.taxa_realizada), 1) AS taxa_media,
          ROUND(AVG(f.meta), 1) AS meta_media,
          ROUND(AVG(f.gap_meta), 1) AS gap_medio,
          ROUND(100 * AVG(CAST(f.meta_atingida AS INT64)), 1) AS pct_atingiu
        FROM `{p}.gold.fato_alfabetizacao` f
        JOIN `{p}.gold.dim_municipio` m ON f.id_municipio = m.id_municipio
        JOIN `{p}.gold.dim_uf` u ON m.sigla_uf = u.sigla_uf
        WHERE f.gap_meta IS NOT NULL
        GROUP BY 1, 2, 3
    """,
    "vw_municipio": """
        SELECT
          m.nome_municipio,
          m.sigla_uf,
          u.regiao,
          f.ano,
          f.taxa_realizada,
          f.meta,
          f.gap_meta,
          f.meta_atingida
        FROM `{p}.gold.fato_alfabetizacao` f
        JOIN `{p}.gold.dim_municipio` m ON f.id_municipio = m.id_municipio
        JOIN `{p}.gold.dim_uf` u ON m.sigla_uf = u.sigla_uf
        WHERE f.gap_meta IS NOT NULL
    """,
}


def main():
    client = bigquery.Client(project=GCP_PROJECT_ID)
    log.info("=== Criando views da camada Gold ===")
    for nome, corpo in VIEWS.items():
        ddl = f"CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.gold.{nome}` AS {corpo.format(p=GCP_PROJECT_ID)}"
        client.query(ddl).result()
        n = pandas_gbq.read_gbq(
            f"SELECT COUNT(*) AS n FROM `{GCP_PROJECT_ID}.gold.{nome}`",
            project_id=GCP_PROJECT_ID, progress_bar_type=None,
        )["n"][0]
        log.info(f"[view] gold.{nome} criada ({n} linhas)")
    log.info("=== Views concluidas ===")


if __name__ == "__main__":
    main()
