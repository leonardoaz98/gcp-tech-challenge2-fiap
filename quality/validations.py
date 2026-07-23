"""
Validacoes de qualidade de dados.

Funcoes reutilizaveis aplicadas na promocao entre camadas. Cada validacao
devolve um relatorio estruturado em vez de apenas logar, permitindo que o
chamador decida se bloqueia ou apenas registra a ocorrencia.
"""

from dataclasses import dataclass, field

import pandas as pd

from config.logger import get_logger

log = get_logger("quality")


@dataclass
class ResultadoValidacao:
    """Relatorio de qualidade de uma tabela."""

    tabela: str
    linhas: int
    duplicatas_chave: int = 0
    nulos_em_chave: int = 0
    pct_nulo_medio: float = 0.0
    chave_usada: list = field(default_factory=list)

    @property
    def aprovado(self) -> bool:
        """Bloqueia promocao se houver duplicata ou chave nula."""
        return self.duplicatas_chave == 0 and self.nulos_em_chave == 0

    def to_dict(self) -> dict:
        return {
            "tabela": self.tabela,
            "linhas": self.linhas,
            "duplicatas_chave": self.duplicatas_chave,
            "nulos_em_chave": self.nulos_em_chave,
            "pct_nulo_medio": self.pct_nulo_medio,
            "aprovado": self.aprovado,
        }


def validar_tabela(
    df: pd.DataFrame,
    nome: str,
    chaves: list,
) -> ResultadoValidacao:
    """
    Roda as checagens padrao sobre uma tabela.

    Verifica duplicidade na chave de negocio, nulos nas colunas-chave e
    calcula o percentual medio de valores ausentes.
    """
    chaves_existentes = [c for c in chaves if c in df.columns]

    resultado = ResultadoValidacao(
        tabela=nome,
        linhas=len(df),
        pct_nulo_medio=round(df.isna().mean().mean() * 100, 2),
        chave_usada=chaves_existentes,
    )

    if chaves_existentes:
        resultado.duplicatas_chave = int(
            df.duplicated(subset=chaves_existentes).sum()
        )
        resultado.nulos_em_chave = int(
            df[chaves_existentes].isna().any(axis=1).sum()
        )

        if resultado.duplicatas_chave:
            log.warning(
                f"[{nome}] {resultado.duplicatas_chave} duplicatas "
                f"em {chaves_existentes}"
            )
        if resultado.nulos_em_chave:
            log.warning(
                f"[{nome}] {resultado.nulos_em_chave} linhas com chave nula"
            )

    if resultado.aprovado:
        log.info(f"[{nome}] validacao OK ({resultado.linhas} linhas)")

    return resultado


def validar_integridade_referencial(
    filha: pd.DataFrame,
    nome_filha: str,
    dimensao: pd.DataFrame,
    chave: str,
) -> set:
    """
    Confere se as chaves da tabela filha existem na dimensao.

    Devolve o conjunto de chaves orfas — vazio quando a integridade esta OK.
    """
    if chave not in filha.columns or chave not in dimensao.columns:
        log.warning(f"[integridade] '{chave}' ausente em {nome_filha} ou dimensao")
        return set()

    universo = set(dimensao[chave].dropna().unique())
    orfas = set(filha[chave].dropna().unique()) - universo

    if orfas:
        exemplos = sorted(orfas)[:3]
        log.warning(
            f"[integridade] {nome_filha}: {len(orfas)} '{chave}' "
            f"ausentes na dimensao (ex: {exemplos})"
        )
    else:
        log.info(f"[integridade] {nome_filha}: OK contra {chave}")

    return orfas


def relatorio_consolidado(resultados: list) -> pd.DataFrame:
    """Monta um DataFrame com o resumo de todas as validacoes."""
    return pd.DataFrame([r.to_dict() for r in resultados])


def bloquear_se_reprovado(resultados: list) -> None:
    """
    Interrompe o pipeline se alguma validacao falhar.

    Principio: falha de qualidade bloqueia a promocao entre camadas.
    """
    reprovadas = [r.tabela for r in resultados if not r.aprovado]
    if reprovadas:
        raise ValueError(
            f"Validacao reprovada em: {', '.join(reprovadas)}. "
            f"Promocao entre camadas bloqueada."
        )
