"""
Camada Gold - Modelagem Dimensional

Le a Silver do BigQuery, enriquece com os diretorios territoriais do IBGE
e materializa um modelo dimensional (fato + dimensoes) pronto para
dashboards, analise estatistica e treino de modelos.
"""

import basedosdados as bd
import pandas as pd
import pandas_gbq

from config.logger import get_logger
from config.settings import (
    BQ_DATASET_GOLD,
    BQ_DATASET_SILVER,
    GCP_PROJECT_ID,
    validar_config,
)
from quality.validations import relatorio_consolidado, validar_tabela

log = get_logger("gold")

ANO_ULTIMO_RESULTADO = 2024

# As metas do Compromisso Nacional cobrem apenas a rede municipal.
# Codigo 3 = Municipal, conforme dicionario da Base dos Dados.
REDE_MUNICIPAL = "3"

# Faixas oficiais do indicador, derivadas de nivel_alfabetizacao
FAIXAS_NIVEL = {
    0: ("Critico", "0 a 39,9%"),
    1: ("Muito baixo", "40 a 49,9%"),
    2: ("Baixo", "50 a 59,9%"),
    3: ("Medio", "60 a 69,9%"),
    4: ("Alto", "70 a 79,9%"),
    5: ("Muito alto", "80 a 100%"),
}


# ----------------------------------------------------------------------
# Leitura
# ----------------------------------------------------------------------

def ler_silver(tabela: str) -> pd.DataFrame:
    """Le uma tabela do dataset Silver."""
    query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.{tabela}`"
    df = pandas_gbq.read_gbq(query, project_id=GCP_PROJECT_ID, progress_bar_type=None)
    log.info(f"[silver/{tabela}] {len(df)} linhas lidas")
    return df


def ler_diretorio_ibge(tabela: str, colunas: list) -> pd.DataFrame:
    """Le o diretorio territorial do IBGE na Base dos Dados."""
    cols = ", ".join(colunas)
    query = f"SELECT {cols} FROM `basedosdados.br_bd_diretorios_brasil.{tabela}`"
    df = bd.read_sql(query=query, billing_project_id=GCP_PROJECT_ID)
    log.info(f"[ibge/{tabela}] {len(df)} linhas lidas")
    return df


# ----------------------------------------------------------------------
# Dimensoes
# ----------------------------------------------------------------------

def construir_dim_uf() -> pd.DataFrame:
    """Dimensao de unidades federativas com regiao."""
    dim = ler_diretorio_ibge("uf", ["sigla", "nome", "regiao"])
    dim = dim.rename(columns={"sigla": "sigla_uf", "nome": "nome_uf"})
    dim["sigla_uf"] = dim["sigla_uf"].str.strip().str.upper()
    log.info(f"[dim_uf] {len(dim)} UFs")
    return dim


def construir_dim_municipio() -> pd.DataFrame:
    """Dimensao de municipios enriquecida com hierarquia territorial."""
    colunas = [
        "id_municipio",
        "nome",
        "sigla_uf",
        "nome_mesorregiao",
        "nome_microrregiao",
        "nome_regiao_imediata",
        "nome_regiao_metropolitana",
        "amazonia_legal",
    ]
    dim = ler_diretorio_ibge("municipio", colunas)

    dim = dim.rename(columns={"nome": "nome_municipio"})
    dim["id_municipio"] = dim["id_municipio"].astype(str).str.strip().str.zfill(7)
    dim["sigla_uf"] = dim["sigla_uf"].str.strip().str.upper()
    dim["em_regiao_metropolitana"] = dim["nome_regiao_metropolitana"].notna()

    log.info(f"[dim_municipio] {len(dim)} municipios")
    return dim


def construir_dim_tempo(anos: list) -> pd.DataFrame:
    """
    Dimensao temporal.

    Classifica cada ano conforme a disponibilidade de dado: 'historico'
    para anos com resultado medido, 'projecao' para anos que possuem
    apenas meta definida.
    """
    dim = pd.DataFrame({"ano": sorted(anos)})
    dim["tipo_ano"] = dim["ano"].apply(
        lambda a: "historico" if a <= ANO_ULTIMO_RESULTADO else "projecao"
    )
    dim["anos_ate_meta_final"] = 2030 - dim["ano"]
    log.info(f"[dim_tempo] {len(dim)} anos ({dim['ano'].min()}-{dim['ano'].max()})")
    return dim


def construir_dim_nivel() -> pd.DataFrame:
    """Dimensao das faixas oficiais de desempenho do indicador."""
    dim = pd.DataFrame(
        [
            {"nivel_alfabetizacao": nivel, "descricao_nivel": desc, "faixa_taxa": faixa}
            for nivel, (desc, faixa) in FAIXAS_NIVEL.items()
        ]
    )
    log.info(f"[dim_nivel] {len(dim)} niveis")
    return dim


# ----------------------------------------------------------------------
# Fato
# ----------------------------------------------------------------------

def construir_fato(
    municipio_resultado: pd.DataFrame,
    meta_municipio: pd.DataFrame,
) -> pd.DataFrame:
    """
    Fato na granularidade municipio x ano.

    As metas do Compromisso Nacional sao definidas apenas para a rede
    municipal, enquanto os resultados vem segmentados por rede. Filtramos
    os resultados para a rede municipal antes de unir, garantindo que os
    dois lados falem da mesma populacao.

    O FULL OUTER JOIN preserva anos que existem em apenas um dos lados:
    2023 tem resultado sem meta, 2025-2030 tem meta sem resultado.
    """
    resultado = (
        municipio_resultado[municipio_resultado["rede"] == REDE_MUNICIPAL][
            ["id_municipio", "ano", "taxa_alfabetizacao", "media_portugues"]
        ].rename(columns={"taxa_alfabetizacao": "taxa_realizada"})
    )
    log.info(f"[fato] resultados da rede municipal: {len(resultado)} linhas")

    meta = meta_municipio[
        ["id_municipio", "ano_meta", "meta_alfabetizacao", "percentual_participacao"]
    ].rename(columns={"ano_meta": "ano", "meta_alfabetizacao": "meta"})

    fato = resultado.merge(meta, on=["id_municipio", "ano"], how="outer")
    fato["rede"] = "municipal"

    fato["gap_meta"] = (fato["taxa_realizada"] - fato["meta"]).round(2)
    fato["meta_atingida"] = fato["gap_meta"].ge(0).where(fato["gap_meta"].notna())
    fato["ano"] = fato["ano"].astype("Int64")

    log.info(
        f"[fato] {len(fato)} linhas | "
        f"com resultado: {fato['taxa_realizada'].notna().sum()} | "
        f"com meta: {fato['meta'].notna().sum()} | "
        f"com ambos: {fato['gap_meta'].notna().sum()}"
    )
    return fato


def anexar_nivel(fato: pd.DataFrame, meta_municipio: pd.DataFrame) -> pd.DataFrame:
    """Traz o nivel oficial de alfabetizacao para a fato."""
    if "nivel_alfabetizacao" not in meta_municipio.columns:
        log.warning("[fato] nivel_alfabetizacao ausente na Silver — pulando")
        return fato

    nivel = meta_municipio[["id_municipio", "nivel_alfabetizacao"]].drop_duplicates(
        subset=["id_municipio"]
    )
    return fato.merge(nivel, on="id_municipio", how="left")


# ----------------------------------------------------------------------
# Escrita
# ----------------------------------------------------------------------

def gravar_gold(df: pd.DataFrame, tabela: str) -> None:
    destino = f"{BQ_DATASET_GOLD}.{tabela}"
    pandas_gbq.to_gbq(
        df,
        destination_table=destino,
        project_id=GCP_PROJECT_ID,
        if_exists="replace",
        progress_bar=False,
    )
    log.info(f"[gold] {destino} gravado ({len(df)} linhas)")


# ----------------------------------------------------------------------
# Pipeline
# ----------------------------------------------------------------------

def main() -> None:
    validar_config()
    log.info("=== Iniciando construcao da camada Gold ===")
    resultados = []

    # --- Leitura da Silver ---
    municipio_resultado = ler_silver("municipio_resultado")
    meta_municipio = ler_silver("meta_municipio")

    # --- Dimensoes ---
    dim_uf = construir_dim_uf()
    dim_municipio = construir_dim_municipio()
    dim_nivel = construir_dim_nivel()

    # --- Fato ---
    fato = construir_fato(municipio_resultado, meta_municipio)
    fato = anexar_nivel(fato, meta_municipio)

    dim_tempo = construir_dim_tempo(fato["ano"].dropna().unique().tolist())

    # --- Validacoes ---
    resultados.append(validar_tabela(dim_uf, "dim_uf", ["sigla_uf"]))
    resultados.append(validar_tabela(dim_municipio, "dim_municipio", ["id_municipio"]))
    resultados.append(validar_tabela(dim_tempo, "dim_tempo", ["ano"]))
    resultados.append(validar_tabela(dim_nivel, "dim_nivel", ["nivel_alfabetizacao"]))
    resultados.append(
        validar_tabela(fato, "fato_alfabetizacao", ["id_municipio", "ano"])
    )

    # --- Escrita ---
    gravar_gold(dim_uf, "dim_uf")
    gravar_gold(dim_municipio, "dim_municipio")
    gravar_gold(dim_tempo, "dim_tempo")
    gravar_gold(dim_nivel, "dim_nivel")
    gravar_gold(fato, "fato_alfabetizacao")

    # --- Relatorio ---
    log.info("=== Relatorio de qualidade ===")
    print(relatorio_consolidado(resultados).to_string(index=False))
    log.info("=== Gold concluida ===")


if __name__ == "__main__":
    main()
