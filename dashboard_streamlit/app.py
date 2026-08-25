"""
app.py — Dashboard VITA (Streamlit)
======================================
Painel de Vigilância Inteligente de Tendências e Atendimentos, consumindo
diretamente as views de negócio do Amazon Athena (camada Gold do datalake).

Substitui/complementa o Power BI: mesmo conteúdo do protótipo (Visão Geral,
Visão Evolutiva, Mapa de Pressão, Alertas), mais o "Ask VITA" (chat com
Bedrock) integrado na mesma tela — um único app, rodando localmente ou
publicado com link.

Como rodar:
  pip install -r requirements.txt
  streamlit run app.py

Pré-requisitos:
  - Credenciais AWS configuradas (mesmas do resto do projeto: `aws configure`)
  - As views do Athena já criadas (analytics/athena_gold_views.sql)
  - Acesso ao Amazon Bedrock habilitado na conta (para a aba "Ask VITA")
"""
import os
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="VITA — Vigilância Inteligente de Tendências e Atendimentos",
    page_icon="🏥",
    layout="wide",
)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "vita_gold")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-6"
)

# ---------------------------------------------------------------------------
# Filtro fixo deste recorte do dashboard: Dengue clássica (A90), SP, 2023-2025
# (A91 = Dengue hemorrágica — excluído a pedido, foco só em A90)
# ---------------------------------------------------------------------------
UF_FILTRO = "SP"
ANO_INICIO_FILTRO = 2023
ANO_FIM_FILTRO = 2025
CIDS_DENGUE = ("A90",)
CIDS_DENGUE_SQL = "(" + ",".join(f"'{c}'" for c in CIDS_DENGUE) + ")"

boto3.setup_default_session(region_name=AWS_REGION)

NIVEL_COR = {
    "Baixo risco": "#2ecc71",
    "Atenção": "#f1c40f",
    "Crítico": "#e67e22",
    "Sobrecarga iminente": "#e74c3c",
}

# Paleta vermelho/vinho — mesma família da escala "Reds" usada nos gráficos
# de barra e no mapa, pra manter consistência visual em todo o dashboard.
# (Só tem A90 no filtro agora, mas mantenho mais de 1 cor pra reaproveitar
# a paleta caso o filtro volte a incluir mais de uma doença no futuro.)
PALETA_VERMELHO_VINHO = [
    "#c0392b",  # vermelho — A90, Dengue clássica
    "#6e2c00",  # vinho/marrom escuro
    "#e74c3c",  # vermelho mais claro
    "#922b21",  # vinho médio
]


# ---------------------------------------------------------------------------
# Camada de dados (cacheada — não bate no Athena a cada interação)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner="Consultando o Athena...")
def query(sql: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql=sql, database=ATHENA_DATABASE, workgroup=ATHENA_WORKGROUP,
        ctas_approach=False,
    )


@st.cache_data(ttl=300)
def carregar_acompanhamento_geral() -> pd.DataFrame:
    return query(f"""
        SELECT
            f.ano, f.mes, h.uf, m.nome_municipio,
            COUNT(*) AS qtd_internacoes,
            SUM(f.valor_total) AS valor_total_pago,
            AVG(f.dias_permanencia) AS permanencia_media
        FROM fato_internacoes f
        JOIN dim_hospital h ON h.cnes_id = f.cnes_id
        LEFT JOIN municipios_referencia m ON m.codigo_municipio = h.codigo_municipio
        WHERE f.cid_principal IN {CIDS_DENGUE_SQL}
          AND h.uf = '{UF_FILTRO}'
          AND f.ano BETWEEN {ANO_INICIO_FILTRO} AND {ANO_FIM_FILTRO}
        GROUP BY f.ano, f.mes, h.uf, m.nome_municipio
    """)


@st.cache_data(ttl=300)
def carregar_tendencia_doencas() -> pd.DataFrame:
    return query(f"""
        SELECT * FROM vw_tendencia_doencas
        WHERE cid_principal IN {CIDS_DENGUE_SQL}
          AND uf = '{UF_FILTRO}'
          AND ano BETWEEN {ANO_INICIO_FILTRO} AND {ANO_FIM_FILTRO}
    """)


@st.cache_data(ttl=300)
def carregar_pressao_por_regiao() -> pd.DataFrame:
    # Não usamos vw_pressao_por_regiao aqui porque o índice de pressão ali é
    # calculado misturando TODAS as doenças, não só dengue. Em vez disso,
    # contamos direto o volume de casos de dengue por município.
    return query(f"""
        SELECT
            h.uf, m.nome_municipio, h.latitude, h.longitude, m.regiao_saude,
            COUNT(*) AS qtd_casos_dengue,
            CAST(COUNT(*) AS DOUBLE) / NULLIF(m.populacao, 0) * 1000 AS casos_por_1000hab
        FROM fato_internacoes f
        JOIN dim_hospital h ON h.cnes_id = f.cnes_id
        LEFT JOIN municipios_referencia m ON m.codigo_municipio = h.codigo_municipio
        WHERE f.cid_principal IN {CIDS_DENGUE_SQL}
          AND h.uf = '{UF_FILTRO}'
          AND f.ano BETWEEN {ANO_INICIO_FILTRO} AND {ANO_FIM_FILTRO}
        GROUP BY h.uf, m.nome_municipio, h.latitude, h.longitude, m.regiao_saude, m.populacao
    """)


@st.cache_data(ttl=300)
def carregar_ranking_criticos() -> pd.DataFrame:
    # Idem: ranking por VOLUME DE CASOS DE DENGUE (não pelo índice de pressão
    # geral, que mistura todas as doenças).
    return query(f"""
        SELECT
            h.nome_estabelecimento, h.uf, m.nome_municipio,
            COUNT(*) AS qtd_casos_dengue,
            AVG(f.dias_permanencia) AS permanencia_media,
            RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_criticidade
        FROM fato_internacoes f
        JOIN dim_hospital h ON h.cnes_id = f.cnes_id
        LEFT JOIN municipios_referencia m ON m.codigo_municipio = h.codigo_municipio
        WHERE f.cid_principal IN {CIDS_DENGUE_SQL}
          AND h.uf = '{UF_FILTRO}'
          AND f.ano BETWEEN {ANO_INICIO_FILTRO} AND {ANO_FIM_FILTRO}
        GROUP BY h.nome_estabelecimento, h.uf, m.nome_municipio
        ORDER BY qtd_casos_dengue DESC
        LIMIT 20
    """)


# ---------------------------------------------------------------------------
# Índice de Pressão Hospitalar (Machine Learning — K-Means)
# Granularidade: hospital x mês, TODAS AS DOENÇAS (não é filtrado por dengue,
# de propósito — é uma leitura geral de capacidade hospitalar, complementar
# ao recorte de dengue do resto do dashboard). Usa as views que já existiam
# no Athena desde o início do projeto, só nunca tinham sido ligadas na UI.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def carregar_indice_pressao_mapa() -> pd.DataFrame:
    return query(f"SELECT * FROM vw_pressao_por_regiao WHERE uf = '{UF_FILTRO}'")


@st.cache_data(ttl=300)
def carregar_indice_pressao_ranking() -> pd.DataFrame:
    return query(f"""
        SELECT * FROM vw_ranking_hospitais_criticos
        WHERE uf = '{UF_FILTRO}'
        ORDER BY ano DESC, mes DESC, ranking_criticidade
        LIMIT 30
    """)


# ---------------------------------------------------------------------------
# Cabeçalho
# ---------------------------------------------------------------------------
col_titulo, col_logo = st.columns([5, 1])
with col_titulo:
    st.title("🏥 VITA")
    st.caption("Vigilância Inteligente de Tendências e Atendimentos — Challenge Oracle & FIAP 2026")
    st.info(
        f"🔎 Filtro ativo: **Dengue** (CID-10 {', '.join(CIDS_DENGUE)}) · "
        f"**{UF_FILTRO}** · **{ANO_INICIO_FILTRO}–{ANO_FIM_FILTRO}**"
    )

aba_geral, aba_evolutiva, aba_mapa, aba_alertas, aba_pressao, aba_chat = st.tabs(
    ["📊 Visão Geral", "📈 Visão Evolutiva", "🗺️ Mapa de Casos", "🚨 Hospitais Mais Afetados",
     "🧠 Pressão Hospitalar (Geral)", "💬 Ask VITA"]
)

# ---------------------------------------------------------------------------
# ABA 1 — Visão Geral (KPIs de dengue)
# ---------------------------------------------------------------------------
with aba_geral:
    df_geral = carregar_acompanhamento_geral()

    if df_geral.empty:
        st.warning("Nenhum caso de dengue encontrado para SP no período 2023-2025. "
                    "Confira se a camada Gold tem dado desses anos.")
    else:
        total_internacoes = int(df_geral["qtd_internacoes"].sum())
        valor_total = df_geral["valor_total_pago"].sum()
        permanencia_media = df_geral["permanencia_media"].mean()
        municipios_afetados = df_geral["nome_municipio"].nunique()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Internações por dengue", f"{total_internacoes:,}".replace(",", "."))
        c2.metric("Valor total pago", f"R$ {valor_total:,.0f}".replace(",", "."))
        c3.metric("Permanência média", f"{permanencia_media:.1f} dias")
        c4.metric("Municípios afetados", municipios_afetados)

        st.divider()

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Casos de dengue por município")
            top_municipios = (
                df_geral.groupby("nome_municipio")["qtd_internacoes"]
                .sum().sort_values(ascending=False).head(15).reset_index()
            )
            fig = px.bar(
                top_municipios, x="qtd_internacoes", y="nome_municipio",
                orientation="h", labels={"qtd_internacoes": "Internações por dengue", "nome_municipio": ""},
                color="qtd_internacoes", color_continuous_scale="Reds",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            st.subheader("Evolução anual")
            por_ano = df_geral.groupby("ano")["qtd_internacoes"].sum().reset_index()
            fig2 = px.bar(
                por_ano, x="ano", y="qtd_internacoes",
                labels={"ano": "Ano", "qtd_internacoes": "Internações por dengue"},
                color="qtd_internacoes", color_continuous_scale="Reds",
            )
            fig2.update_xaxes(type="category")
            st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# ABA 2 — Visão Evolutiva (linha do tempo mensal de dengue)
# ---------------------------------------------------------------------------
with aba_evolutiva:
    df_tend = carregar_tendencia_doencas()

    if df_tend.empty:
        st.warning("Nenhum dado retornado.")
    else:
        df_tend = df_tend.copy()
        df_tend["periodo"] = pd.to_datetime(
            df_tend["ano"].astype(str) + "-" + df_tend["mes"].astype(str) + "-01"
        )
        df_tend = df_tend.sort_values("periodo")

        fig = px.line(
            df_tend, x="periodo", y="qtd_casos_mes", color="cid_principal",
            markers=True, labels={"periodo": "Mês", "qtd_casos_mes": "Casos de dengue", "cid_principal": "CID-10"},
            color_discrete_sequence=PALETA_VERMELHO_VINHO,
        )
        fig.update_traces(line=dict(width=3), marker=dict(size=7))
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Ver tabela de variação mês a mês"):
            st.dataframe(
                df_tend[["periodo", "cid_principal", "qtd_casos_mes", "variacao_media_pct"]]
                .sort_values("periodo", ascending=False),
                use_container_width=True,
            )

# ---------------------------------------------------------------------------
# ABA 3 — Mapa de Casos de Dengue por Região
# ---------------------------------------------------------------------------
with aba_mapa:
    df_mapa = carregar_pressao_por_regiao()

    if df_mapa.empty:
        st.warning("Nenhum dado retornado.")
    else:
        df_mapa = df_mapa.dropna(subset=["latitude", "longitude"])
        fig = px.scatter_map(
            df_mapa, lat="latitude", lon="longitude",
            color="qtd_casos_dengue", size="qtd_casos_dengue",
            color_continuous_scale="Reds",
            hover_name="nome_municipio",
            hover_data={"qtd_casos_dengue": True, "casos_por_1000hab": ":.2f",
                        "latitude": False, "longitude": False},
            zoom=6, height=600, map_style="open-street-map",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Tamanho e cor do círculo = quantidade de internações por dengue no município.")

# ---------------------------------------------------------------------------
# ABA 4 — Hospitais mais afetados por dengue (ranking por volume)
# ---------------------------------------------------------------------------
with aba_alertas:
    df_alertas = carregar_ranking_criticos()

    if df_alertas.empty:
        st.info("Nenhum caso de dengue encontrado no período.")
    else:
        st.subheader(f"Top {len(df_alertas)} hospitais com mais internações por dengue (SP, 2023–2025)")

        fig = px.bar(
            df_alertas.sort_values("qtd_casos_dengue"),
            x="qtd_casos_dengue", y="nome_estabelecimento", orientation="h",
            labels={"qtd_casos_dengue": "Internações por dengue", "nome_estabelecimento": ""},
            color="qtd_casos_dengue", color_continuous_scale="Reds",
        )
        fig.update_layout(showlegend=False, height=600)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            df_alertas[["ranking_criticidade", "nome_estabelecimento", "nome_municipio",
                        "qtd_casos_dengue", "permanencia_media"]]
            .rename(columns={
                "ranking_criticidade": "#", "nome_estabelecimento": "Hospital",
                "nome_municipio": "Município", "qtd_casos_dengue": "Casos de dengue",
                "permanencia_media": "Permanência média (dias)",
            }),
            use_container_width=True, hide_index=True,
        )

# ---------------------------------------------------------------------------
# ABA 5 — Índice de Pressão Hospitalar (Machine Learning, todas as doenças)
# ---------------------------------------------------------------------------
with aba_pressao:
    st.caption(
        "⚠️ Esta aba **não é filtrada por dengue** — mostra a capacidade hospitalar geral "
        "(todas as doenças), calculada via **K-Means (scikit-learn)** sobre volume de "
        "internações, permanência média e proporção de pacientes críticos. "
        "É uma visão complementar às demais abas, focadas só em dengue."
    )

    df_mapa_pressao = carregar_indice_pressao_mapa()
    df_ranking_pressao = carregar_indice_pressao_ranking()

    if df_mapa_pressao.empty and df_ranking_pressao.empty:
        st.warning("Nenhum dado retornado. Confira se o job `vita-silver-to-gold` já rodou "
                    "e se as views `vw_pressao_por_regiao`/`vw_ranking_hospitais_criticos` existem.")
    else:
        # --- KPIs de resumo ---
        if not df_ranking_pressao.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Índice de pressão médio", f"{df_ranking_pressao['indice_pressao'].mean():.1f} / 100")
            criticos = (df_ranking_pressao["nivel_risco"] == "Sobrecarga iminente").sum()
            c2.metric("Hospitais em Sobrecarga iminente", int(criticos))
            c3.metric("Hospitais monitorados", df_ranking_pressao["nome_estabelecimento"].nunique())

        st.divider()

        col_a, col_b = st.columns([3, 2])
        with col_a:
            st.subheader("Mapa de pressão hospitalar por região")
            if df_mapa_pressao.empty:
                st.info("Sem dados geográficos suficientes.")
            else:
                df_m = df_mapa_pressao.dropna(subset=["latitude", "longitude"])
                fig_mapa = px.scatter_map(
                    df_m, lat="latitude", lon="longitude",
                    color="nivel_risco_regiao", size="indice_pressao_medio",
                    color_discrete_map=NIVEL_COR,
                    hover_name="nome_municipio",
                    hover_data={"indice_pressao_medio": ":.1f", "internacoes_por_1000hab": ":.2f",
                                "latitude": False, "longitude": False},
                    zoom=6, height=500, map_style="open-street-map",
                )
                fig_mapa.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig_mapa, use_container_width=True)

        with col_b:
            st.subheader("Distribuição por nível de risco")
            if df_ranking_pressao.empty:
                st.info("Sem dados suficientes.")
            else:
                contagem = df_ranking_pressao["nivel_risco"].value_counts().reset_index()
                contagem.columns = ["nivel_risco", "qtd_hospitais"]
                fig_pizza = px.pie(
                    contagem, names="nivel_risco", values="qtd_hospitais",
                    color="nivel_risco", color_discrete_map=NIVEL_COR, hole=0.45,
                )
                st.plotly_chart(fig_pizza, use_container_width=True)

        st.divider()
        st.subheader("Ranking de hospitais críticos (todas as doenças)")
        if df_ranking_pressao.empty:
            st.success("Nenhum hospital em nível Crítico ou Sobrecarga iminente no momento. ✅")
        else:
            for _, linha in df_ranking_pressao.head(20).iterrows():
                cor = NIVEL_COR.get(linha["nivel_risco"], "#95a5a6")
                st.markdown(
                    f"""
                    <div style="border-left: 6px solid {cor}; padding: 10px 16px; margin-bottom: 8px;
                                background-color: rgba(0,0,0,0.03); border-radius: 4px;">
                        <b>{linha['nome_estabelecimento']}</b> — {linha['nome_municipio']}/{linha['uf']}<br/>
                        Nível: <b style="color:{cor}">{linha['nivel_risco']}</b> |
                        Índice de pressão: <b>{linha['indice_pressao']:.1f}</b> |
                        Internações no mês: {int(linha['qtd_internacoes'])} |
                        Permanência média: {linha['permanencia_media']:.1f} dias
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

# ---------------------------------------------------------------------------
# ABA 6 — Ask VITA (Bedrock: pergunta em português -> SQL -> resposta)
# ---------------------------------------------------------------------------
with aba_chat:
    st.caption(
        "Pergunte em português sobre os dados do VITA. O Claude (via Amazon Bedrock) "
        "converte a pergunta em SQL, executa no Athena e responde em linguagem natural "
        "— equivalente funcional ao Oracle Select AI."
    )

    SCHEMA_CONTEXT = """
Tabelas disponíveis (schema vita_gold, Amazon Athena):

vw_acompanhamento_geral(ano, mes, uf, nome_municipio, qtd_internacoes, valor_total_pago,
                         permanencia_media, indice_pressao_medio, nivel_risco_predominante)

vw_tendencia_doencas(uf, cid_principal, ano, mes, qtd_casos_mes, variacao_media_pct)

vw_pressao_por_regiao(uf, nome_municipio, latitude, longitude, regiao_saude,
                       indice_pressao_medio, nivel_risco_regiao, internacoes_por_1000hab)

vw_ranking_hospitais_criticos(nome_estabelecimento, uf, nome_municipio, ano, mes,
                               indice_pressao, nivel_risco, qtd_internacoes,
                               permanencia_media, ranking_criticidade)
"""

    if "historico_chat" not in st.session_state:
        st.session_state.historico_chat = []

    for autor, texto in st.session_state.historico_chat:
        with st.chat_message(autor):
            st.markdown(texto)

    pergunta = st.chat_input("Ex.: Quais hospitais estão com maior índice de pressão?")

    if pergunta:
        st.session_state.historico_chat.append(("user", pergunta))
        with st.chat_message("user"):
            st.markdown(pergunta)

        with st.chat_message("assistant"):
            with st.spinner("Consultando..."):
                try:
                    bedrock = boto3.client("bedrock-runtime")

                    prompt_sql = f"""Você converte perguntas em português para SQL do Amazon Athena (dialeto Presto/Trino).

{SCHEMA_CONTEXT}

Regras: gere APENAS SELECT (nunca DDL/DML), responda SOMENTE com o SQL (sem markdown),
use LIMIT 20 se a pergunta não pedir agregação total.

Pergunta: {pergunta}
SQL:"""
                    resp = bedrock.invoke_model(
                        modelId=BEDROCK_MODEL_ID,
                        body=__import__("json").dumps({
                            "anthropic_version": "bedrock-2023-05-31",
                            "max_tokens": 400,
                            "messages": [{"role": "user", "content": prompt_sql}],
                        }),
                    )
                    sql_gerado = __import__("json").loads(resp["body"].read())["content"][0]["text"].strip()

                    st.code(sql_gerado, language="sql")
                    resultado = query(sql_gerado)

                    prompt_resposta = f"""Pergunta: "{pergunta}"

Resultado (JSON): {resultado.head(20).to_json(orient="records", force_ascii=False)}

Responda em português, 2-3 frases, direto ao ponto, como um analista de dados
explicando o resultado a um gestor hospitalar."""
                    resp2 = bedrock.invoke_model(
                        modelId=BEDROCK_MODEL_ID,
                        body=__import__("json").dumps({
                            "anthropic_version": "bedrock-2023-05-31",
                            "max_tokens": 300,
                            "messages": [{"role": "user", "content": prompt_resposta}],
                        }),
                    )
                    texto_resposta = __import__("json").loads(resp2["body"].read())["content"][0]["text"].strip()

                    st.markdown(texto_resposta)
                    st.dataframe(resultado, use_container_width=True)
                    st.session_state.historico_chat.append(("assistant", texto_resposta))

                except Exception as e:  # noqa: BLE001
                    erro = f"Não consegui responder essa pergunta: {e}"
                    st.error(erro)
                    st.session_state.historico_chat.append(("assistant", erro))

st.divider()
st.caption(f"VITA · Dados via Amazon Athena · Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M')}")
