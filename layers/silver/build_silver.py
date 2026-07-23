"""
Camada Silver - Tratamento e Integracao
Le a Bronze do disco local, aplica limpeza e padronizacao, faz o unpivot
das metas (wide -> long) e integra as bases pelas chaves territoriais.
Grava o resultado no BigQuery (dataset silver).
"""

import os
import glob
import logging
import re

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "silver")
BRONZE_DIR = "data/bronze"
ANOS_META = [2024, 2025, 2026, 2027, 2028, 2029, 2030]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("silver")


# ----------------------------------------------------------------------
# Leitura
# ----------------------------------------------------------------------

def ler_bronze(tabela: str, com_ano: bool = True) -> pd.DataFrame:
    """
    Le os Parquet de uma tabela da Bronze.

    Tabelas sem coluna 'ano' real foram replicadas em cada diretorio de
    particao na ingestao. Nesses casos (com_ano=False), lemos apenas um
    arquivo e deduplicamos, evitando multiplicar os registros.
    """
    arquivos = sorted(
        glob.glob(f"{BRONZE_DIR}/{tabela}/**/*.parquet", recursive=True)
    )
    if not arquivos:
        raise FileNotFoundError(f"Nenhum Parquet encontrado para '{tabela}'")

    if com_ano:
        partes = []
        for caminho in arquivos:
            parte = pd.read_parquet(caminho)
            match = re.search(r"ano=(\d{4})", caminho)
            if match and "ano" not in parte.columns:
                parte["ano"] = int(match.group(1))
            partes.append(parte)
        df = pd.concat(partes, ignore_index=True)
    else:
        df = pd.read_parquet(arquivos[0])
        antes = len(df)
        df = df.drop_duplicates().reset_index(drop=True)
        if len(df) < antes:
            log.info(f"[bronze/{tabela}] {antes - len(df)} duplicatas removidas")

    log.info(f"[bronze/{tabela}] {df.shape[0]} linhas lidas")
    return df


# ----------------------------------------------------------------------
# Padronizacao
# ----------------------------------------------------------------------

def padronizar(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos e valores de texto das chaves e categorias."""
    df = df.copy()

    if "id_municipio" in df.columns:
        df["id_municipio"] = (
            df["id_municipio"].astype(str).str.strip().str.zfill(7)
        )

    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].astype(str).str.strip().str.upper()

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    for col in ["rede", "serie"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    return df


def remover_colunas_constantes(df: pd.DataFrame, protegidas: list) -> pd.DataFrame:
    """Descarta colunas com um unico valor, exceto as protegidas."""
    constantes = [
        c for c in df.columns
        if c not in protegidas and df[c].nunique(dropna=False) <= 1
    ]
    if constantes:
        log.info(f"  removendo colunas constantes: {constantes}")
    return df.drop(columns=constantes)


# ----------------------------------------------------------------------
# Unpivot das metas
# ----------------------------------------------------------------------

def unpivot_metas(df: pd.DataFrame, chaves: list, nivel: str) -> pd.DataFrame:
    """Converte meta_alfabetizacao_2024..2030 de colunas para linhas."""
    cols_meta = [f"meta_alfabetizacao_{a}" for a in ANOS_META]
    cols_meta = [c for c in cols_meta if c in df.columns]

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
# Validacoes de qualidade
# ----------------------------------------------------------------------

def validar(df: pd.DataFrame, nome: str, chaves: list) -> dict:
    """Roda checagens de qualidade e devolve um relatorio."""
    rel = {"tabela": nome, "linhas": len(df)}

    chaves_existentes = [c for c in chaves if c in df.columns]
    if chaves_existentes:
        dups = df.duplicated(subset=chaves_existentes).sum()
        rel["duplicatas_chave"] = int(dups)
        if dups:
            log.warning(f"[{nome}] {dups} duplicatas em {chaves_existentes}")

        nulos_chave = int(df[chaves_existentes].isna().any(axis=1).sum())
        rel["nulos_em_chave"] = nulos_chave
        if nulos_chave:
            log.warning(f"[{nome}] {nulos_chave} linhas com chave nula")

    rel["pct_nulo_medio"] = round(df.isna().mean().mean() * 100, 2)
    return rel


def validar_integridade(filhas: dict, dim: pd.DataFrame, chave: str) -> None:
    """Confere se as chaves das tabelas filhas existem na dimensao."""
    universo = set(dim[chave].dropna().unique())
    for nome, df in filhas.items():
        if chave not in df.columns:
            continue
        orfas = set(df[chave].dropna().unique()) - universo
        if orfas:
            log.warning(
                f"[integridade] {nome}: {len(orfas)} '{chave}' "
                f"ausentes na dimensao (ex: {list(orfas)[:3]})"
            )
        else:
            log.info(f"[integridade] {nome}: OK contra {chave}")


# ----------------------------------------------------------------------
# Escrita
# ----------------------------------------------------------------------

def gravar_bigquery(df: pd.DataFrame, tabela: str) -> None:
    destino = f"{DATASET_SILVER}.{tabela}"
    df.to_gbq(
        destination_table=destino,
        project_id=PROJECT,
        if_exists="replace",
        progress_bar=False,
    )
    log.info(f"[silver] {destino} gravado ({len(df)} linhas)")


# ----------------------------------------------------------------------
# Pipeline
# ----------------------------------------------------------------------

def main():
    log.info("=== Iniciando construcao da camada Silver ===")
    relatorio = []

    # --- Dimensao municipio (resultados por municipio e ano) ---
    municipio = padronizar(ler_bronze("municipio"))
    municipio = remover_colunas_constantes(municipio, ["id_municipio", "ano"])
    municipio = municipio.drop(columns=["_ingestao_timestamp", "_fonte"], errors="ignore")
    relatorio.append(validar(municipio, "municipio", ["id_municipio", "ano", "rede"]))

    # --- Resultados por UF ---
    uf = padronizar(ler_bronze("uf"))
    uf = remover_colunas_constantes(uf, ["sigla_uf", "ano"])
    uf = uf.drop(columns=["_ingestao_timestamp", "_fonte"], errors="ignore")
    relatorio.append(validar(uf, "uf", ["sigla_uf", "rede", "ano"]))

    # --- Metas: unpivot wide -> long ---
    meta_mun_raw = padronizar(ler_bronze("meta_alfabetizacao_municipio", com_ano=False))
    meta_uf_raw = padronizar(ler_bronze("meta_alfabetizacao_uf", com_ano=False))
    meta_br_raw = padronizar(ler_bronze("meta_alfabetizacao_brasil", com_ano=False))

    meta_mun = unpivot_metas(
        meta_mun_raw,
        ["id_municipio", "rede", "taxa_alfabetizacao", "percentual_participacao"],
        "municipio",
    )
    meta_uf = unpivot_metas(
        meta_uf_raw,
        ["sigla_uf", "rede", "taxa_alfabetizacao", "percentual_participacao"],
        "uf",
    )
    meta_br = unpivot_metas(
        meta_br_raw,
        ["rede", "taxa_alfabetizacao", "percentual_participacao"],
        "brasil",
    )

    relatorio.append(validar(meta_mun, "meta_municipio", ["id_municipio", "ano_meta"]))
    relatorio.append(validar(meta_uf, "meta_uf", ["sigla_uf", "ano_meta"]))

    # --- Integridade referencial ---
    validar_integridade(
        {"meta_alfabetizacao_municipio": meta_mun},
        municipio,
        "id_municipio",
    )
    validar_integridade(
        {"meta_alfabetizacao_uf": meta_uf},
        uf,
        "sigla_uf",
    )

    # --- Grava no BigQuery ---
    gravar_bigquery(municipio, "municipio_resultado")
    gravar_bigquery(uf, "uf_resultado")
    gravar_bigquery(meta_mun, "meta_municipio")
    gravar_bigquery(meta_uf, "meta_uf")
    gravar_bigquery(meta_br, "meta_brasil")

    # --- Relatorio final ---
    log.info("=== Relatorio de qualidade ===")
    print(pd.DataFrame(relatorio).to_string(index=False))
    log.info("=== Silver concluida ===")


if __name__ == "__main__":
    main()
