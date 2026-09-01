"""
Exploracao dos schemas da camada Bronze.

Etapa de Data Understanding do CRISP-DM: le os Parquet da Bronze local e
reporta colunas, tipos, nulos e cardinalidade das chaves de integracao.
Foi a partir daqui que os achados registrados em
`docs/decisoes_arquiteturais.md` foram identificados.

Uso:
    python -m exploration.explorar_schemas
"""

import glob

import pandas as pd

from config.logger import get_logger
from config.settings import BRONZE_DIR, TABELAS_BRONZE

log = get_logger("explora")

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

CHAVES_INTEGRACAO = ["id_municipio", "sigla_uf", "ano"]


def carregar(tabela: str) -> pd.DataFrame:
    """Concatena todos os Parquet de uma tabela da Bronze."""
    padrao = str(BRONZE_DIR / tabela / "**" / "*.parquet")
    arquivos = glob.glob(padrao, recursive=True)

    if not arquivos:
        raise FileNotFoundError(f"Nenhum Parquet encontrado para '{tabela}'")

    return pd.concat(
        [pd.read_parquet(a) for a in arquivos], ignore_index=True
    )


def perfilar(tabela: str) -> None:
    """Imprime o perfil de uma tabela."""
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

    for chave in CHAVES_INTEGRACAO:
        if chave in df.columns:
            exemplos = df[chave].dropna().head(3).tolist()
            print(
                f"  -> {chave}: {df[chave].nunique()} valores unicos | "
                f"exemplo: {exemplos}"
            )
    print()


def main() -> None:
    """Perfila todas as tabelas da Bronze, isolando falhas por tabela."""
    for tabela in TABELAS_BRONZE:
        try:
            perfilar(tabela)
        except FileNotFoundError as erro:
            log.warning(f"[{tabela}] {erro}")


if __name__ == "__main__":
    main()
