"""
Dashboard executivo - Compromisso Nacional Crianca Alfabetizada.

Le exclusivamente as views da camada Gold. Nenhuma regra de negocio vive
aqui: agregacao e criterio de media sao responsabilidade da Gold.

Criterio de media: sempre municipal. Os KPIs reagregam a partir de
`soma_taxa` / `qtd_com_taxa`, nunca da media das medias por UF - do
contrario o numero nacional nao fecha com o municipal.
"""

import sys
from pathlib import Path

# streamlit run coloca dashboard/ no sys.path, nao a raiz do projeto.
# Sem isso, o fallback 'from config.settings import ...' falha com
# ModuleNotFoundError fora do Streamlit Cloud, onde st.secrets nao existe.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pandas_gbq
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Alfabetização Municipal", layout="wide")

# Credenciais: st.secrets no Streamlit Cloud, config local no desenvolvimento.
try:
    from google.oauth2 import service_account

    _sa = dict(st.secrets["gcp_service_account"])
    CREDS = service_account.Credentials.from_service_account_info(_sa)
    GCP_PROJECT_ID = _sa["project_id"]
except Exception:
    CREDS = None
    from config.settings import GCP_PROJECT_ID

GOLD = f"{GCP_PROJECT_ID}.gold"

# Realizado e meta precisam ser visualmente inconfundiveis: sao naturezas
# diferentes de dado, nao duas series comparaveis do mesmo tipo.
COR_REALIZADO = "#2E9E5B"
COR_META = "#E8853B"
PALETA = {"Realizado": COR_REALIZADO, "Meta": COR_META}
ANO_ULTIMO_RESULTADO = 2024


@st.cache_data(ttl=3600)
def consultar(sql: str) -> pd.DataFrame:
    return pandas_gbq.read_gbq(
        sql, project_id=GCP_PROJECT_ID, credentials=CREDS, progress_bar_type=None
    )


def media_municipal(df: pd.DataFrame, soma: str, qtd: str) -> float:
    """
    Reagrega a media no nivel municipal a partir de soma e contagem.

    Retorna NaN quando nao ha nenhum municipio com o valor preenchido -
    caso legitimo, nao erro: 2023 nao tem meta e 2025+ nao tem resultado.
    """
    denominador = df[qtd].sum()
    return df[soma].sum() / denominador if denominador else float("nan")


def formatar(valor: float, sufixo: str = "%", sinal: bool = False) -> str:
    if pd.isna(valor):
        return "—"
    fmt = f"{valor:+.1f}" if sinal else f"{valor:.1f}"
    return f"{fmt}{sufixo}"


# ----------------------------------------------------------------------
# Cabecalho e filtros
# ----------------------------------------------------------------------

st.title("Alfabetização na Rede Municipal")
st.caption("Compromisso Nacional Criança Alfabetizada — dados INEP / IBGE")

uf_df = consultar(f"SELECT * FROM `{GOLD}.vw_uf_ano`")

anos = sorted(uf_df["ano"].dropna().unique())
ano_padrao = anos.index(2024) if 2024 in anos else len(anos) - 1
ano = st.sidebar.selectbox("Ano", anos, index=ano_padrao)

regioes = ["Todas"] + sorted(uf_df["regiao"].dropna().unique().tolist())
regiao = st.sidebar.selectbox("Região", regioes)

tipo_ano = uf_df.loc[uf_df["ano"] == ano, "tipo_ano"].iloc[0]
if tipo_ano == "projecao":
    st.sidebar.info(
        f"{ano} é um ano de projeção: existe meta definida, "
        "mas ainda não há resultado medido."
    )


def aplicar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    """Filtro unico aplicado a todos os blocos da pagina."""
    out = df[df["ano"] == ano]
    if regiao != "Todas":
        out = out[out["regiao"] == regiao]
    return out


dados_uf = aplicar_filtros(uf_df)

# ----------------------------------------------------------------------
# KPIs
# ----------------------------------------------------------------------

taxa = media_municipal(dados_uf, "soma_taxa", "qtd_com_taxa")
meta = media_municipal(dados_uf, "soma_meta", "qtd_com_meta")
comparaveis = dados_uf["qtd_comparavel"].sum()
pct_atingiu = (
    100 * dados_uf["qtd_atingiu"].sum() / comparaveis if comparaveis else float("nan")
)
gap = taxa - meta if not (pd.isna(taxa) or pd.isna(meta)) else float("nan")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Taxa média realizada", formatar(taxa))
c2.metric("Meta média", formatar(meta))
c3.metric("Gap vs meta", formatar(gap, " p.p.", sinal=True))
c4.metric("Municípios atingindo a meta", formatar(pct_atingiu))

st.caption(
    f"Base: {int(dados_uf['municipios'].sum())} municípios | "
    f"com resultado: {int(dados_uf['qtd_com_taxa'].sum())} | "
    f"com meta: {int(dados_uf['qtd_com_meta'].sum())} | "
    f"comparáveis: {int(comparaveis)}. "
    "Médias reagregadas no nível municipal."
)

st.divider()

# ----------------------------------------------------------------------
# Ranking por UF
# ----------------------------------------------------------------------

ranking = dados_uf.copy()
ranking["taxa_municipal"] = (
    ranking["soma_taxa"] / ranking["qtd_com_taxa"].replace(0, pd.NA)
).round(1)
ranking = ranking.dropna(subset=["taxa_municipal"])

if ranking.empty:
    st.info(f"Não há resultado medido em {ano} — apenas meta definida.")
else:
    fig = px.bar(
        ranking.sort_values("taxa_municipal"),
        x="taxa_municipal",
        y="sigla_uf",
        color="regiao",
        orientation="h",
        title=f"Taxa média de alfabetização por UF — {ano}",
        labels={"taxa_municipal": "Taxa (%)", "sigla_uf": "UF", "regiao": "Região"},
    )
    fig.update_layout(height=650, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")

# ----------------------------------------------------------------------
# Recorte regional — respeita os mesmos filtros do topo
# ----------------------------------------------------------------------

regiao_df = aplicar_filtros(consultar(f"SELECT * FROM `{GOLD}.vw_regiao_ano`"))

comparativo = regiao_df.assign(
    Realizado=lambda d: (d["soma_taxa"] / d["qtd_com_taxa"].replace(0, pd.NA)).round(1),
    Meta=lambda d: (d["soma_meta"] / d["qtd_com_meta"].replace(0, pd.NA)).round(1),
)

# Os dois graficos ficam lado a lado: uma ordem comum evita que o leitor
# tenha que reprocessar a sequencia das regioes ao comparar um com o outro.
chave_ordem = "Realizado" if comparativo["Realizado"].notna().any() else "Meta"
ordem_regioes = (
    comparativo.sort_values(chave_ordem, ascending=False)["regiao"].dropna().tolist()
)
CATEGORIAS = {"regiao": ordem_regioes}

col_a, col_b = st.columns(2)

with col_a:
    fig2 = px.bar(
        comparativo,
        x="regiao",
        y=["Realizado", "Meta"],
        barmode="group",
        title=f"Realizado vs meta por região — {ano}",
        labels={"value": "Taxa (%)", "regiao": "Região", "variable": ""},
        color_discrete_map=PALETA,
        category_orders=CATEGORIAS,
    )
    st.plotly_chart(fig2, width="stretch")

with col_b:
    atingimento = regiao_df.assign(
        pct=lambda d: (
            100 * d["qtd_atingiu"] / d["qtd_comparavel"].replace(0, pd.NA)
        ).round(1)
    ).dropna(subset=["pct"])

    if atingimento.empty:
        st.info(f"Sem comparação meta x resultado em {ano}.")
    else:
        fig3 = px.bar(
            atingimento,
            x="regiao",
            y="pct",
            title=f"% de municípios que atingiram a meta — {ano}",
            labels={"pct": "% atingiu", "regiao": "Região"},
            category_orders=CATEGORIAS,
        )
        fig3.update_traces(marker_color=COR_REALIZADO)
        st.plotly_chart(fig3, width="stretch")

st.divider()

# ----------------------------------------------------------------------
# Trajetoria historica ate 2030
# ----------------------------------------------------------------------

st.subheader("Trajetória até a meta de 2030")

serie_base = consultar(f"SELECT * FROM `{GOLD}.vw_regiao_ano`")
if regiao != "Todas":
    serie_base = serie_base[serie_base["regiao"] == regiao]

serie = (
    serie_base.groupby("ano")[
        ["soma_taxa", "qtd_com_taxa", "soma_meta", "qtd_com_meta"]
    ]
    .sum()
    .reset_index()
    .assign(
        Realizado=lambda d: (d["soma_taxa"] / d["qtd_com_taxa"].replace(0, pd.NA)).round(1),
        Meta=lambda d: (d["soma_meta"] / d["qtd_com_meta"].replace(0, pd.NA)).round(1),
    )
)

fig4 = px.line(
    serie,
    x="ano",
    y=["Realizado", "Meta"],
    markers=True,
    title=f"Evolução do indicador — {regiao.lower() if regiao != 'Todas' else 'Brasil'}",
    labels={"value": "Taxa (%)", "ano": "Ano", "variable": ""},
    color_discrete_map=PALETA,
)

# Realizado e meta se encostam em 2024 (62.8 vs 62.2). Com a mesma cor e o
# mesmo tracado, o grafico vira uma unica linha subindo ate 80% e sugere que
# a trajetoria futura ja esta contratada. Nao esta: de 2025 em diante e meta
# pactuada, nao desempenho medido. O tracejado e o sombreado tornam a
# fronteira explicita para quem le rapido.
fig4.for_each_trace(
    lambda t: t.update(line={"dash": "dash", "width": 2})
    if t.name == "Meta"
    else t.update(line={"width": 3})
)

fig4.add_vrect(
    x0=ANO_ULTIMO_RESULTADO,
    x1=serie["ano"].max(),
    fillcolor=COR_META,
    opacity=0.06,
    line_width=0,
    annotation_text="meta pactuada",
    annotation_position="top left",
    annotation_font_size=11,
)

fig4.add_vline(
    x=ANO_ULTIMO_RESULTADO,
    line_dash="dot",
    line_color="#8A8A8A",
    line_width=1,
)

st.plotly_chart(fig4, width="stretch")

st.caption(
    "A área sombreada marca o período de meta pactuada, sem resultado medido. "
    "A série realizada termina em 2024 — a descontinuidade é esperada, "
    "não falha de dado."
)

st.divider()

# ----------------------------------------------------------------------
# Detalhe municipal
# ----------------------------------------------------------------------

st.subheader(f"Municípios — {ano}")

mun = consultar(f"SELECT * FROM `{GOLD}.vw_municipio` WHERE ano = {ano}")
if regiao != "Todas":
    mun = mun[mun["regiao"] == regiao]

comparavel = mun.dropna(subset=["gap_meta"])

if comparavel.empty:
    st.info(
        f"Em {ano} não há comparação meta x resultado. "
        "Exibindo apenas os valores disponíveis."
    )
    tabela = mun
else:
    # Deficit vem primeiro de proposito. O topo da lista por superavit e
    # dominado por gaps acima de 50 p.p., que a nota metodologica abaixo
    # classifica como meta mal calibrada - ou seja, o painel abriria
    # destacando justamente o dado menos confiavel. Deficit tambem e onde
    # mora a decisao de politica publica.
    ordem = st.radio(
        "Ordenar por gap:",
        ["Maiores déficits", "Maiores superávits"],
        horizontal=True,
    )
    tabela = comparavel.sort_values(
        "gap_meta", ascending=(ordem == "Maiores déficits")
    )

st.dataframe(
    tabela[["nome_municipio", "sigla_uf", "regiao", "taxa_realizada", "meta", "gap_meta"]],
    width="stretch",
    height=400,
    hide_index=True,
    column_config={
        "nome_municipio": "Município",
        "sigla_uf": "UF",
        "regiao": "Região",
        "taxa_realizada": st.column_config.NumberColumn("Realizado (%)", format="%.1f"),
        "meta": st.column_config.NumberColumn("Meta (%)", format="%.1f"),
        "gap_meta": st.column_config.NumberColumn("Gap (p.p.)", format="%+.1f"),
    },
)

st.caption(
    "Nota metodológica: gaps acima de ~50 p.p. geralmente indicam metas mal "
    "calibradas em municípios de pequeno porte, não desempenho excepcional."
)
