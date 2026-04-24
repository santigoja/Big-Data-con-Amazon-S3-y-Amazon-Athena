import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Big Data · S3 & Athena",
    page_icon="🏥",
    layout="wide",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stSidebar"] { background: #0f1117; }
    [data-testid="stSidebar"] * { color: #e0e0e0 !important; }
    .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_athena_client(region, key_id, secret_key, session_token=None):
    kwargs = dict(
        region_name=region,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
    )
    if session_token:
        kwargs["aws_session_token"] = session_token
    return boto3.client("athena", **kwargs)


def run_query(athena, sql: str, database: str, output_location: str):
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    qid = resp["QueryExecutionId"]

    with st.spinner("Consultando Athena…"):
        for _ in range(120):
            status = athena.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", state)
        return None, reason

    paginator = athena.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=qid)
    rows, header = [], None
    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        if header is None:
            header = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([c.get("VarCharValue", "") for c in row["Data"]])

    df = pd.DataFrame(rows, columns=header) if header else pd.DataFrame()
    return df, None


# ── Session state ─────────────────────────────────────────────────────────────
for k in ["athena", "connected", "bucket", "database", "output_location",
          "region", "key_id", "secret_key", "session_tok"]:
    if k not in st.session_state:
        st.session_state[k] = None if k != "connected" else False

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg",
        width=110,
    )
    st.title("Conexión AWS")
    st.divider()

    region      = st.text_input("Región", value="us-east-1")
    key_id      = st.text_input("Access Key ID",     type="password")
    secret_key  = st.text_input("Secret Access Key", type="password")
    session_tok = st.text_input("Session Token (opcional)", type="password")

    st.divider()
    bucket   = st.text_input("Bucket S3",            placeholder="estudiante-bigdata-2026")
    database = st.text_input("Base de datos Athena",  value="salud_bigdata")

    if st.button("🔌 Conectar", use_container_width=True):
        if not all([region, key_id, secret_key, bucket, database]):
            st.error("Completa todos los campos.")
        else:
            try:
                client = get_athena_client(
                    region, key_id, secret_key,
                    session_tok if session_tok else None,
                )
                client.list_work_groups()
                st.session_state.athena          = client
                st.session_state.connected       = True
                st.session_state.bucket          = bucket
                st.session_state.database        = database
                st.session_state.output_location = f"s3://{bucket}/athena-results/"
                st.session_state.region          = region
                st.session_state.key_id          = key_id
                st.session_state.secret_key      = secret_key
                st.session_state.session_tok     = session_tok
                st.success("✅ Conectado")
            except Exception as e:
                st.error(f"Error: {e}")

    if st.session_state.connected:
        st.markdown(
            f"**● Conectado**  \n"
            f"`{st.session_state.bucket}` · `{st.session_state.database}`"
        )

# ── MAIN ──────────────────────────────────────────────────────────────────────
st.title("🏥 Big Data — Visualización de Consultas Athena")
st.caption("Amazon S3 · Amazon Athena · salud_bigdata")

if not st.session_state.connected:
    st.info("👈 Configura tu conexión AWS en la barra lateral y pulsa **Conectar**.")
    st.stop()

athena   = st.session_state.athena
bucket   = st.session_state.bucket
database = st.session_state.database
output   = st.session_state.output_location

col_run, col_info = st.columns([2, 5])
with col_run:
    run_all = st.button("▶ Ejecutar todas las consultas", use_container_width=True, type="primary")
with col_info:
    st.caption(f"Base: `{database}` · Resultados: `{output}`")

st.divider()

# ── Helper: ejecutar y cachear ────────────────────────────────────────────────
def get_data(key, sql):
    if run_all or key not in st.session_state:
        df, err = run_query(athena, sql, database, output)
        st.session_state[key] = {"error": err} if err else {"df": df}
    return st.session_state.get(key, {})

# ─────────────────────────────────────────────────────────────────────────────
# 12.1 · Promedio del valor por entidad emisora
# ─────────────────────────────────────────────────────────────────────────────
st.header("12.1 · Promedio del valor por entidad emisora")

result_q1 = get_data("q1", f"""
SELECT entidad_emisora,
       ROUND(AVG(valor_procedimiento), 2) AS promedio_valor
FROM {database}.ordenes_raw_csv
GROUP BY entidad_emisora
ORDER BY promedio_valor DESC;
""")

if "error" in result_q1:
    st.error(result_q1["error"])
elif "df" in result_q1:
    df1 = result_q1["df"].copy()
    df1["promedio_valor"] = pd.to_numeric(df1["promedio_valor"], errors="coerce")

    c1, c2, c3 = st.columns(3)
    c1.metric("Entidades", len(df1))
    c2.metric("Mayor promedio", f"${df1['promedio_valor'].max():,.2f}")
    c3.metric("Menor promedio", f"${df1['promedio_valor'].min():,.2f}")

    col_bar, col_pie = st.columns(2)
    with col_bar:
        fig = px.bar(
            df1, x="entidad_emisora", y="promedio_valor",
            title="Promedio por entidad emisora",
            labels={"entidad_emisora": "Entidad", "promedio_valor": "Promedio ($)"},
            color="promedio_valor", color_continuous_scale="Blues", text_auto=".2f",
        )
        fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    with col_pie:
        fig2 = px.pie(
            df1, names="entidad_emisora", values="promedio_valor",
            title="Distribución del promedio por entidad", hole=0.35,
            color_discrete_sequence=px.colors.sequential.Blues_r,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Ver tabla"):
        st.dataframe(df1, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# 12.2 · JOIN órdenes ↔ eventos
# ─────────────────────────────────────────────────────────────────────────────
st.header("12.2 · Cruce órdenes ↔ eventos clínicos (JOIN)")

result_q2 = get_data("q2", f"""
SELECT o.orden_id, o.fecha, o.entidad_emisora,
       o.valor_procedimiento,
       e.paciente.nombre AS nombre_paciente,
       e.diagnostico, e.ciudad
FROM {database}.ordenes_raw_csv o
JOIN {database}.eventos_clinicos_json e ON o.orden_id = e.orden_id;
""")

if "error" in result_q2:
    st.error(result_q2["error"])
elif "df" in result_q2:
    df2 = result_q2["df"].copy()
    df2["valor_procedimiento"] = pd.to_numeric(df2["valor_procedimiento"], errors="coerce")

    c1, c2, c3 = st.columns(3)
    c1.metric("Órdenes cruzadas",    len(df2))
    c2.metric("Ciudades",            df2["ciudad"].nunique())
    c3.metric("Diagnósticos únicos", df2["diagnostico"].nunique())

    col_bar, col_pie = st.columns(2)
    with col_bar:
        df2_ciudad = (
            df2.groupby("ciudad")["valor_procedimiento"]
            .mean().reset_index()
            .rename(columns={"valor_procedimiento": "promedio"})
            .sort_values("promedio", ascending=False)
        )
        fig = px.bar(
            df2_ciudad, x="ciudad", y="promedio",
            title="Promedio de valor por ciudad",
            labels={"ciudad": "Ciudad", "promedio": "Promedio ($)"},
            color="promedio", color_continuous_scale="Teal", text_auto=".2f",
        )
        fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    with col_pie:
        df2_diag = df2["diagnostico"].value_counts().reset_index()
        df2_diag.columns = ["diagnostico", "cantidad"]
        fig2 = px.pie(
            df2_diag, names="diagnostico", values="cantidad",
            title="Distribución de diagnósticos", hole=0.35,
            color_discrete_sequence=px.colors.sequential.Teal_r,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Ver tabla"):
        st.dataframe(df2, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# 12.3 · UNNEST medicamentos
# ─────────────────────────────────────────────────────────────────────────────
st.header("12.3 · Expansión de medicamentos (UNNEST)")

result_q3 = get_data("q3", f"""
SELECT e.orden_id, medicamento
FROM {database}.eventos_clinicos_json e
CROSS JOIN UNNEST(e.medicamentos) AS t(medicamento);
""")

if "error" in result_q3:
    st.error(result_q3["error"])
elif "df" in result_q3:
    df3 = result_q3["df"].copy()

    c1, c2 = st.columns(2)
    c1.metric("Registros expandidos",  len(df3))
    c2.metric("Medicamentos únicos",   df3["medicamento"].nunique())

    top15 = df3["medicamento"].value_counts().head(15).reset_index()
    top15.columns = ["medicamento", "frecuencia"]

    col_bar, col_pie = st.columns(2)
    with col_bar:
        fig = px.bar(
            top15, x="medicamento", y="frecuencia",
            title="Top 15 medicamentos (UNNEST)",
            labels={"medicamento": "Medicamento", "frecuencia": "Frecuencia"},
            color="frecuencia", color_continuous_scale="Purples", text_auto=True,
        )
        fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

    with col_pie:
        fig2 = px.pie(
            top15, names="medicamento", values="frecuencia",
            title="Proporción Top 15 medicamentos", hole=0.35,
            color_discrete_sequence=px.colors.sequential.Purples_r,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Ver tabla"):
        st.dataframe(df3, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# 12.4 · Frecuencia de medicamentos
# ─────────────────────────────────────────────────────────────────────────────
st.header("12.4 · Frecuencia de medicamentos")

result_q4 = get_data("q4", f"""
SELECT medicamento, COUNT(*) AS frecuencia
FROM {database}.eventos_clinicos_json e
CROSS JOIN UNNEST(e.medicamentos) AS t(medicamento)
GROUP BY medicamento
ORDER BY frecuencia DESC;
""")

if "error" in result_q4:
    st.error(result_q4["error"])
elif "df" in result_q4:
    df4 = result_q4["df"].copy()
    df4["frecuencia"] = pd.to_numeric(df4["frecuencia"], errors="coerce")
    top10 = df4.head(10)

    c1, c2, c3 = st.columns(3)
    c1.metric("Medicamentos distintos", len(df4))
    c2.metric("Más frecuente",  df4.iloc[0]["medicamento"] if not df4.empty else "-")
    c3.metric("Frecuencia máx", int(df4["frecuencia"].max()) if not df4.empty else 0)

    col_bar, col_pie = st.columns(2)
    with col_bar:
        fig = px.bar(
            top10, x="medicamento", y="frecuencia",
            title="Top 10 medicamentos más recetados",
            labels={"medicamento": "Medicamento", "frecuencia": "Frecuencia"},
            color="frecuencia", color_continuous_scale="Oranges", text_auto=True,
        )
        fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    with col_pie:
        fig2 = px.pie(
            top10, names="medicamento", values="frecuencia",
            title="Proporción Top 10 medicamentos", hole=0.35,
            color_discrete_sequence=px.colors.sequential.Oranges_r,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Ver tabla completa"):
        st.dataframe(df4, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Capa curada · Diagnósticos + costo promedio
# ─────────────────────────────────────────────────────────────────────────────
st.header("✨ Capa curada · Diagnósticos y costo promedio")

result_q5 = get_data("q5", f"""
SELECT diagnostico,
       COUNT(*) AS casos,
       ROUND(AVG(valor_procedimiento), 2) AS costo_promedio
FROM {database}.ordenes_curadas_parquet
GROUP BY diagnostico
ORDER BY casos DESC;
""")

if "error" in result_q5:
    st.error(result_q5["error"])
elif "df" in result_q5:
    df5 = result_q5["df"].copy()
    df5["casos"]          = pd.to_numeric(df5["casos"],          errors="coerce")
    df5["costo_promedio"] = pd.to_numeric(df5["costo_promedio"], errors="coerce")

    c1, c2, c3 = st.columns(3)
    c1.metric("Diagnósticos distintos",    len(df5))
    c2.metric("Diagnóstico más frecuente", df5.iloc[0]["diagnostico"] if not df5.empty else "-")
    c3.metric("Costo promedio general",    f"${df5['costo_promedio'].mean():,.2f}" if not df5.empty else "-")

    col_bar, col_pie = st.columns(2)
    with col_bar:
        fig = px.bar(
            df5, x="diagnostico", y="casos",
            title="Casos por diagnóstico",
            labels={"diagnostico": "Diagnóstico", "casos": "Casos"},
            color="costo_promedio", color_continuous_scale="RdYlGn_r", text_auto=True,
            color_continuous_midpoint=df5["costo_promedio"].mean(),
        )
        fig.update_layout(xaxis_tickangle=-35, coloraxis_colorbar_title="Costo prom.")
        st.plotly_chart(fig, use_container_width=True)

    with col_pie:
        fig2 = px.pie(
            df5, names="diagnostico", values="casos",
            title="Proporción de casos por diagnóstico", hole=0.35,
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

    # Gráfica extra: costo promedio por diagnóstico
    fig3 = px.bar(
        df5.sort_values("costo_promedio", ascending=False),
        x="diagnostico", y="costo_promedio",
        title="Costo promedio por diagnóstico",
        labels={"diagnostico": "Diagnóstico", "costo_promedio": "Costo promedio ($)"},
        color="costo_promedio", color_continuous_scale="Blues", text_auto=".2f",
    )
    fig3.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35)
    st.plotly_chart(fig3, use_container_width=True)

    with st.expander("Ver tabla"):
        st.dataframe(df5, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Big Data · Amazon S3 + Amazon Athena · Streamlit + Plotly")
