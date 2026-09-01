"""
Views analiticas da camada Gold consumidas pelo dashboard.

Principios de modelagem aplicados aqui:

1. Nenhum filtro que descarte linha estruturalmente valida. A fato cobre
   tres casos temporais distintos - 2023 tem resultado sem meta, 2024 tem
   os dois, 2025-2030 tem meta sem resultado. Um `WHERE gap_meta IS NOT
   NULL` reduziria a serie inteira a 2024 silenciosamente.

2. Toda view agregada carrega `soma_*` e `qtd_*` alem da media. Media de
   media nao e reagregavel: a media nacional calculada a partir das medias
   por UF diverge da media calculada sobre os municipios. Com soma e
   contagem, o consumidor reagrega em qualquer nivel sem perder precisao.

3. Os joins com as dimensoes sao LEFT. Um municipio ausente do diretorio
   do IBGE deve aparecer com regiao nula, nao sumir da contagem.

Criterio oficial do indicador: a media e sempre municipal. O municipio e a
unidade de gestao da politica publica, entao UF e regiao sao agregacoes de
municipios - nunca medias de medias.
"""

import pandas_gbq
from google.cloud import bigquery

from config.logger import get_logger
from config.settings import BQ_DATASET_GOLD, GCP_PROJECT_ID, validar_config

log = get_logger("views")


# Bloco de metricas reutilizado pelas views agregadas.
# soma_* / qtd_* permitem reagregacao correta em qualquer nivel.
METRICAS = """
          COUNT(*) AS municipios,

          SUM(f.taxa_realizada) AS soma_taxa,
          COUNTIF(f.taxa_realizada IS NOT NULL) AS qtd_com_taxa,

          SUM(f.meta) AS soma_meta,
          COUNTIF(f.meta IS NOT NULL) AS qtd_com_meta,

          COUNTIF(f.meta_atingida) AS qtd_atingiu,
          COUNTIF(f.meta_atingida IS NOT NULL) AS qtd_comparavel,

          ROUND(AVG(f.taxa_realizada), 1) AS taxa_media,
          ROUND(AVG(f.meta), 1) AS meta_media,
          ROUND(AVG(f.gap_meta), 1) AS gap_medio
"""

JOINS = """
        FROM `{p}.{g}.fato_alfabetizacao` f
        LEFT JOIN `{p}.{g}.dim_municipio` m ON f.id_municipio = m.id_municipio
        LEFT JOIN `{p}.{g}.dim_uf` u ON m.sigla_uf = u.sigla_uf
        LEFT JOIN `{p}.{g}.dim_tempo` t ON f.ano = t.ano
"""

VIEWS = {
    "vw_uf_ano": """
        SELECT
          u.sigla_uf,
          u.regiao,
          f.ano,
          t.tipo_ano,
""" + METRICAS + JOINS + """
        GROUP BY 1, 2, 3, 4
    """,

    "vw_regiao_ano": """
        SELECT
          u.regiao,
          f.ano,
          t.tipo_ano,
""" + METRICAS + JOINS + """
        GROUP BY 1, 2, 3
    """,

    "vw_municipio": """
        SELECT
          f.id_municipio,
          m.nome_municipio,
          m.sigla_uf,
          u.regiao,
          f.ano,
          t.tipo_ano,
          f.taxa_realizada,
          f.meta,
          f.gap_meta,
          f.meta_atingida,
          f.nivel_alfabetizacao
""" + JOINS,
}


def diagnosticar(nome: str) -> None:
    """
    Loga a cobertura da view por ano.

    Alarme contra silent-drop: se um ano conhecido sumir da contagem,
    aparece aqui antes de chegar ao dashboard.
    """
    if nome == "vw_municipio":
        sql = f"""
            SELECT ano,
                   COUNT(*) AS linhas,
                   COUNTIF(taxa_realizada IS NOT NULL) AS com_taxa,
                   COUNTIF(meta IS NOT NULL) AS com_meta
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.{nome}`
            GROUP BY ano ORDER BY ano
        """
    else:
        sql = f"""
            SELECT ano,
                   SUM(municipios) AS linhas,
                   SUM(qtd_com_taxa) AS com_taxa,
                   SUM(qtd_com_meta) AS com_meta
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.{nome}`
            GROUP BY ano ORDER BY ano
        """

    df = pandas_gbq.read_gbq(sql, project_id=GCP_PROJECT_ID, progress_bar_type=None)
    log.info(f"[view] {nome} - cobertura por ano:")
    for _, linha in df.iterrows():
        log.info(
            f"         {linha['ano']}: {linha['linhas']} linhas | "
            f"com taxa: {linha['com_taxa']} | com meta: {linha['com_meta']}"
        )


def main() -> None:
    validar_config()
    client = bigquery.Client(project=GCP_PROJECT_ID)
    log.info("=== Criando views da camada Gold ===")

    for nome, corpo in VIEWS.items():
        sql = corpo.format(p=GCP_PROJECT_ID, g=BQ_DATASET_GOLD)
        ddl = (
            f"CREATE OR REPLACE VIEW "
            f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.{nome}` AS {sql}"
        )
        client.query(ddl).result()
        log.info(f"[view] gold.{nome} criada")
        diagnosticar(nome)

    log.info("=== Views concluidas ===")


if __name__ == "__main__":
    main()
