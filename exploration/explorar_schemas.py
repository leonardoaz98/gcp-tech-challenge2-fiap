"""
Exploracao dos schemas da camada Bronze.
Le os Parquet locais e imprime colunas, tipos, nulos e cardinalidade
das chaves de integracao.
"""

import glob
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

TABELAS = [
    "uf",
    "municipio",
    "alunos",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]


def carregar(tabela: str) -> pd.DataFrame:
    arquivos = glob.glob(f"data/bronze/{tabela}/**/*.parquet", recursive=True)
    return pd.concat([pd.read_parquet(a) for a in arquivos], ignore_index=True)


for tabela in TABELAS:
    df = carregar(tabela)
    print("=" * 90)
    print(f"TABELA: {tabela}  |  {df.shape[0]} linhas x {df.shape[1]} colunas")
    print("-" * 90)

    resumo = pd.DataFrame({
        "tipo": df.dtypes.astype(str),
        "nulos": df.isna().sum(),
        "pct_nulo": (df.isna().mean() * 100).round(2),
        "unicos": df.nunique(),
    })
    print(resumo)

    # Chaves de integracao
    for chave in ["id_municipio", "sigla_uf", "ano"]:
        if chave in df.columns:
            print(f"  -> {chave}: {df[chave].nunique()} valores unicos | "
                  f"exemplo: {df[chave].dropna().head(3).tolist()}")
    print()
