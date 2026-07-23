import streamlit as st
import pandas_gbq
import plotly.express as px

st.set_page_config(page_title="Alfabetizacao Municipal", layout="wide")

# Credenciais e projeto: st.secrets no Cloud, config local
try:
    from google.oauth2 import service_account
    _sa = dict(st.secrets["gcp_service_account"])
    CREDS = service_account.Credentials.from_service_account_info(_sa)
    GCP_PROJECT_ID = _sa["project_id"]
except Exception:
    CREDS = None
    from config.settings import GCP_PROJECT_ID


@st.cache_data(ttl=3600)
def q(sql):
    return pandas_gbq.read_gbq(sql, project_id=GCP_PROJECT_ID, credentials=CREDS, progress_bar_type=None)


st.title("Alfabetizacao na Rede Municipal")
st.caption("Compromisso Nacional Crianca Alfabetizada — dados INEP/IBGE")

uf_df = q(f"SELECT * FROM `{GCP_PROJECT_ID}.gold.vw_uf_ano`")

anos = sorted(uf_df["ano"].unique())
ano = st.sidebar.selectbox("Ano", anos, index=len(anos) - 1)

regioes = ["Todas"] + sorted(uf_df["regiao"].unique().tolist())
regiao = st.sidebar.selectbox("Regiao", regioes)

dados = uf_df[uf_df["ano"] == ano]
if regiao != "Todas":
    dados = dados[dados["regiao"] == regiao]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Taxa media", f"{dados['taxa_media'].mean():.1f}%")
c2.metric("Municipios atingindo meta", f"{dados['pct_atingiu'].mean():.1f}%")
c3.metric("Gap medio", f"{dados['gap_medio'].mean():+.1f} p.p.")
c4.metric("Municipios analisados", int(dados["municipios"].sum()))

st.divider()

fig = px.bar(
    dados.sort_values("taxa_media"),
    x="taxa_media", y="sigla_uf", color="regiao",
    orientation="h",
    title=f"Taxa media de alfabetizacao por UF — {ano}",
    labels={"taxa_media": "Taxa (%)", "sigla_uf": "UF", "regiao": "Regiao"},
)
fig.update_layout(height=650, yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig, use_container_width=True)

col_a, col_b = st.columns(2)

regiao_df = q(f"SELECT * FROM `{GCP_PROJECT_ID}.gold.vw_regiao_ano`")

with col_a:
    fig2 = px.bar(
        regiao_df.sort_values("taxa_media"),
        x="regiao", y=["taxa_media", "meta_media"],
        barmode="group", title="Realizado vs Meta por regiao",
        labels={"value": "Taxa (%)", "regiao": "Regiao", "variable": ""},
    )
    fig2.for_each_trace(
        lambda t: t.update(name="Realizado" if t.name == "taxa_media" else "Meta")
    )
    st.plotly_chart(fig2, use_container_width=True)

with col_b:
    fig3 = px.bar(
        regiao_df.sort_values("pct_atingiu"),
        x="regiao", y="pct_atingiu",
        title="% de municipios que atingiram a meta",
        labels={"pct_atingiu": "% atingiu", "regiao": "Regiao"},
    )
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

st.subheader(f"Municipios — {ano}")
mun = q(f"SELECT * FROM `{GCP_PROJECT_ID}.gold.vw_municipio` WHERE ano = {ano}")
if regiao != "Todas":
    mun = mun[mun["regiao"] == regiao]

ordem = st.radio(
    "Ordenar por gap:",
    ["Maiores superavits", "Maiores deficits"],
    horizontal=True,
)

mun_ord = mun.sort_values("gap_meta", ascending=(ordem == "Maiores deficits"))

st.dataframe(
    mun_ord[["nome_municipio", "sigla_uf", "regiao", "taxa_realizada", "meta", "gap_meta"]],
    use_container_width=True, height=400, hide_index=True,
    column_config={
        "nome_municipio": "Municipio",
        "sigla_uf": "UF",
        "regiao": "Regiao",
        "taxa_realizada": st.column_config.NumberColumn("Realizado (%)", format="%.1f"),
        "meta": st.column_config.NumberColumn("Meta (%)", format="%.1f"),
        "gap_meta": st.column_config.NumberColumn("Gap (p.p.)", format="%+.1f"),
    },
)

st.caption(
    "Nota: gaps extremos (acima de ~50 p.p.) geralmente indicam metas mal calibradas "
    "em municipios de pequeno porte, nao desempenho excepcional."
)
