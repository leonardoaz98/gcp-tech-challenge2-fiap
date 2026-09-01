"""
Camada Silver - Tratamento e Integracao

Le a Bronze do disco local, aplica limpeza e padronizacao, converte as
metas de formato wide para long e grava no BigQuery (dataset silver).
"""

import glob
import re

import pandas as pd
import pandas_gbq

from config.logger import get_logger
from config.settings import (
    ANOS_META,
    BQ_DATASET_SILVER,
    BRONZE_DIR,
    GCP_PROJECT_ID,
    validar_config,
)
from quality.validations import (
    relatorio_consolidado,
    validar_integridade_referencial,
    validar_tabela,
)

log = get_logger("silver")

# Colunas que carregam semantica de negocio e nunca podem ser descartadas
# automaticamente, ainda que constantes num recorte especifico.
COLUNAS_DE_NEGOCIO = {"rede", "serie", "sigla_uf", "id_municipio", "ano"}


# ----------------------------------------------------------------------
# Leitura
# ----------------------------------------------------------------------

def ler_bronze(tabela: str) -> pd.DataFrame:
    """
    Le os Parquet de uma tabela da Bronze.

    A coluna de particao nao e gravada dentro do arquivo pelo Hive layout -
    ela vive no nome do diretorio (`ano=2024/`). Por isso e reconstruida a
    partir do caminho quando ausente no DataFrame.

    Historico: uma versao anterior da ingestao particionava por `ano` mesmo
    em tabelas que nao possuiam a coluna, replicando o dataset inteiro em
    cada diretorio. O bug foi corrigido na origem (ver `gravar_local`), mas
    a deduplicacao permanece como rede de seguranca contra reingestao de
    dado antigo que ainda esteja no bucket.
    """
    padrao = str(BRONZE_DIR / tabela / "**" / "*.parquet")
    arquivos = sorted(glob.glob(padrao, recursive=True))

    if not arquivos:
        raise FileNotFoundError(f"Nenhum Parquet encontrado para '{tabela}'")

    partes = []
    for caminho in arquivos:
        parte = pd.read_parquet(caminho)
        match = re.search(r"ano=(\d{4})", caminho)
        if match and "ano" not in parte.columns:
            parte["ano"] = int(match.group(1))
        partes.append(parte)

    df = pd.concat(partes, ignore_index=True)

    antes = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    if len(df) < antes:
        log.warning(
            f"[bronze/{tabela}] {antes - len(df)} duplicatas removidas "
            f"(possivel residuo de particionamento antigo)"
        )

    log.info(f"[bronze/{tabela}] {df.shape[0]} linhas lidas de {len(arquivos)} arquivo(s)")
    return df


# ----------------------------------------------------------------------
# Padronizacao
# ----------------------------------------------------------------------

def padronizar(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos e valores de texto das chaves e categorias."""
    df = df.copy()

    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str.zfill(7)

    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].astype(str).str.strip().str.upper()

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    for col in ["rede", "serie"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    return df


def remover_colunas_constantes(df: pd.DataFrame, protegidas: list) -> pd.DataFrame:
    """
    Descarta colunas com um unico valor, exceto as protegidas.

    Colunas de negocio nunca sao removidas mesmo quando constantes num
    recorte especifico. `rede`, por exemplo, e o filtro que garante que
    resultados e metas falem da mesma populacao na Gold - se ela sumisse
    da Silver por ser constante, `construir_fato` quebraria com KeyError.
    """
    inegociaveis = set(protegidas) | COLUNAS_DE_NEGOCIO

    constantes = [
        c for c in df.columns
        if c not in inegociaveis and df[c].nunique(dropna=False) <= 1
    ]
    if constantes:
        log.info(f"  removendo colunas constantes: {constantes}")

    preservadas = [
        c for c in df.columns
        if c in COLUNAS_DE_NEGOCIO and df[c].nunique(dropna=False) <= 1
    ]
    if preservadas:
        log.info(f"  constantes preservadas (chave de negocio): {preservadas}")

    return df.drop(columns=constantes)


def limpar_metadados(df: pd.DataFrame) -> pd.DataFrame:
    """Remove as colunas tecnicas de rastreabilidade da Bronze."""
    return df.drop(columns=["_ingestao_timestamp", "_fonte"], errors="ignore")


# ----------------------------------------------------------------------
# Unpivot das metas
# ----------------------------------------------------------------------

def unpivot_metas(df: pd.DataFrame, chaves: list, nivel: str) -> pd.DataFrame:
    """Converte meta_alfabetizacao_2024..2030 de colunas para linhas."""
    cols_meta = [
        f"meta_alfabetizacao_{a}" for a in ANOS_META
        if f"meta_alfabetizacao_{a}" in df.columns
    ]
    id_vars = [c for c in chaves if c in df.columns]

    longo = df.melt(
        id_vars=id_vars,
        value_vars=cols_meta,
        var_name="ano_meta",
        value_name="meta_alfabetizacao",
    )
    longo["ano_meta"] = (
        longo["ano_meta"].str.replace("meta_alfabetizacao_", "").astype(int)
    )
    longo["nivel_territorial"] = nivel

    log.info(f"[metas/{nivel}] unpivot: {df.shape[0]} -> {longo.shape[0]} linhas")
    return longo


# ----------------------------------------------------------------------
# Escrita
# ----------------------------------------------------------------------

def gravar_bigquery(df: pd.DataFrame, tabela: str) -> None:
    """Grava a tabela no dataset Silver do BigQuery."""
    destino = f"{BQ_DATASET_SILVER}.{tabela}"
    pandas_gbq.to_gbq(
        df,
        destination_table=destino,
        project_id=GCP_PROJECT_ID,
        if_exists="replace",
        progress_bar=False,
    )
    log.info(f"[silver] {destino} gravado ({len(df)} linhas)")


# ----------------------------------------------------------------------
# Pipeline
# ----------------------------------------------------------------------

def main() -> None:
    validar_config()
    log.info("=== Iniciando construcao da camada Silver ===")
    resultados = []

    # --- Resultados por municipio ---
    municipio = limpar_metadados(padronizar(ler_bronze("municipio")))
    municipio = remover_colunas_constantes(municipio, ["id_municipio", "ano"])
    resultados.append(
        validar_tabela(municipio, "municipio", ["id_municipio", "ano", "rede"])
    )

    # --- Resultados por UF ---
    uf = limpar_metadados(padronizar(ler_bronze("uf")))
    uf = remover_colunas_constantes(uf, ["sigla_uf", "ano"])
    resultados.append(validar_tabela(uf, "uf", ["sigla_uf", "rede", "ano"]))

    # --- Metas: wide -> long ---
    chaves_comuns = ["rede", "taxa_alfabetizacao", "percentual_participacao"]
    chaves_mun = ["id_municipio", "nivel_alfabetizacao"] + chaves_comuns

    meta_mun = unpivot_metas(
        padronizar(ler_bronze("meta_alfabetizacao_municipio")),
        chaves_mun,
        "municipio",
    )
    meta_uf = unpivot_metas(
        padronizar(ler_bronze("meta_alfabetizacao_uf")),
        ["sigla_uf"] + chaves_comuns,
        "uf",
    )
    meta_br = unpivot_metas(
        padronizar(ler_bronze("meta_alfabetizacao_brasil")),
        chaves_comuns,
        "brasil",
    )

    resultados.append(
        validar_tabela(meta_mun, "meta_municipio", ["id_municipio", "ano_meta"])
    )
    resultados.append(validar_tabela(meta_uf, "meta_uf", ["sigla_uf", "ano_meta"]))

    # --- Integridade referencial ---
    validar_integridade_referencial(meta_mun, "meta_municipio", municipio, "id_municipio")
    validar_integridade_referencial(meta_uf, "meta_uf", uf, "sigla_uf")

    # --- Escrita ---
    gravar_bigquery(municipio, "municipio_resultado")
    gravar_bigquery(uf, "uf_resultado")
    gravar_bigquery(meta_mun, "meta_municipio")
    gravar_bigquery(meta_uf, "meta_uf")
    gravar_bigquery(meta_br, "meta_brasil")

    # --- Relatorio ---
    log.info("=== Relatorio de qualidade ===")
    print(relatorio_consolidado(resultados).to_string(index=False))
    log.info("=== Silver concluida ===")


if __name__ == "__main__":
    main()
