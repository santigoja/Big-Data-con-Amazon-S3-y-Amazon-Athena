import streamlit as st
import boto3
import pandas as pd
import time
import json

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Big Data – S3 & Athena",
    page_icon="🏥",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stSidebar"] { background: #0f1117; }
    [data-testid="stSidebar"] * { color: #e0e0e0 !important; }
    .block-container { padding-top: 1.5rem; }
    .sql-box {
        background: #1e1e2e;
        color: #cdd6f4;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        white-space: pre-wrap;
        border: 1px solid #313244;
        margin-bottom: 0.5rem;
    }
    .status-ok  { color: #a6e3a1; font-weight: 600; }
    .status-err { color: #f38ba8; font-weight: 600; }
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-left: 6px;
    }
    .badge-raw    { background: #313244; color: #cdd6f4; }
    .badge-curada { background: #1e3a5f; color: #89b4fa; }
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
    """Submit query and poll until done. Returns (df | None, error_msg | None)."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    qid = resp["QueryExecutionId"]

    with st.spinner("Ejecutando consulta en Athena…"):
        for _ in range(120):          # max ~2 min
            status = athena.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", state)
        return None, reason

    # Paginate results
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


def ddl_run(athena, sql: str, output_location: str):
    """Run DDL (CREATE, etc.) – no result set needed."""
    resp = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": output_location},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(120):
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        return status["QueryExecution"]["Status"].get("StateChangeReason", state)
    return None

# ── Session state defaults ────────────────────────────────────────────────────
for k in ["athena", "connected", "bucket", "database", "output_location"]:
    if k not in st.session_state:
        st.session_state[k] = None if k != "connected" else False

# ── SIDEBAR – Conexión AWS ────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg",
        width=120,
    )
    st.title("Configuración AWS")
    st.divider()

    region      = st.text_input("Región", value="us-east-1", key="region")
    key_id      = st.text_input("Access Key ID",     type="password")
    secret_key  = st.text_input("Secret Access Key", type="password")
    session_tok = st.text_input("Session Token (opcional)", type="password")

    st.divider()
    bucket_name  = st.text_input("Nombre del bucket S3", placeholder="estudiante-bigdata-2026")
    database     = st.text_input("Base de datos Athena",  value="salud_bigdata")

    if st.button("🔌 Conectar", use_container_width=True):
        if not all([region, key_id, secret_key, bucket_name, database]):
            st.error("Completa todos los campos obligatorios.")
        else:
            try:
                client = get_athena_client(
                    region, key_id, secret_key,
                    session_tok if session_tok else None,
                )
                # quick connectivity test
                client.list_work_groups()
                st.session_state.athena           = client
                st.session_state.connected        = True
                st.session_state.bucket           = bucket_name
                st.session_state.database         = database
                st.session_state.output_location  = f"s3://{bucket_name}/athena-results/"
                st.success("✅ Conectado")
            except Exception as e:
                st.error(f"Error: {e}")

    if st.session_state.connected:
        st.markdown(
            f"<span class='status-ok'>● Conectado</span><br>"
            f"<small>bucket: <code>{st.session_state.bucket}</code><br>"
            f"db: <code>{st.session_state.database}</code></small>",
            unsafe_allow_html=True,
        )

# ── MAIN ──────────────────────────────────────────────────────────────────────
st.title("🏥 Big Data – Amazon S3 & Athena")
st.caption("Ejercicio conducido · carga · tablas externas · consultas · UNNEST · capa curada Parquet")

if not st.session_state.connected:
    st.info("👈 Completa la configuración en la barra lateral y pulsa **Conectar** para empezar.")
    st.stop()

athena    = st.session_state.athena
bucket    = st.session_state.bucket
database  = st.session_state.database
output    = st.session_state.output_location

# ── TABS ──────────────────────────────────────────────────────────────────────
tabs = st.tabs([
    "1 · Setup",
    "2 · Tablas externas",
    "3 · Verificación",
    "4 · Consultas",
    "5 · Capa curada",
    "6 · SQL libre",
])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 · SETUP
# ─────────────────────────────────────────────────────────────────────────────
with tabs[0]:
    st.header("Setup inicial")

    # ── Crear base de datos ──────────────────────────────────────────────
    st.subheader("Crear base de datos")
    sql_create_db = f"CREATE DATABASE IF NOT EXISTS {database};"
    st.markdown(f'<div class="sql-box">{sql_create_db}</div>', unsafe_allow_html=True)

    if st.button("▶ Crear base de datos", key="btn_create_db"):
        err = ddl_run(athena, sql_create_db, output)
        if err:
            st.error(f"Error: {err}")
        else:
            st.success(f"Base `{database}` lista.")

    st.divider()

    # ── Subir archivos a S3 ──────────────────────────────────────────────
    st.subheader("Subir archivos al bucket S3")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**CSV – órdenes**")
        csv_file = st.file_uploader("ordenes_raw_ampliado.csv", type=["csv"], key="up_csv")
        if csv_file and st.button("☁️ Subir CSV", key="btn_up_csv"):
            try:
                s3 = boto3.client(
                    "s3",
                    region_name=region,
                    aws_access_key_id=key_id,
                    aws_secret_access_key=secret_key,
                    **({"aws_session_token": session_tok} if session_tok else {}),
                )
                s3.upload_fileobj(csv_file, bucket, "raw/ordenes_csv/ordenes_raw_ampliado.csv")
                st.success("✅ CSV subido a `raw/ordenes_csv/`")
            except Exception as e:
                st.error(f"Error S3: {e}")

    with col2:
        st.markdown("**JSON Lines – eventos clínicos**")
        json_file = st.file_uploader("eventos_clinicos_ampliado.jsonl", type=["jsonl", "json"], key="up_json")
        if json_file and st.button("☁️ Subir JSONL", key="btn_up_json"):
            try:
                s3 = boto3.client(
                    "s3",
                    region_name=region,
                    aws_access_key_id=key_id,
                    aws_secret_access_key=secret_key,
                    **({"aws_session_token": session_tok} if session_tok else {}),
                )
                s3.upload_fileobj(json_file, bucket, "raw/eventos_json/eventos_clinicos_ampliado.jsonl")
                st.success("✅ JSONL subido a `raw/eventos_json/`")
            except Exception as e:
                st.error(f"Error S3: {e}")

    st.divider()
    st.markdown(f"""
**Estructura esperada en el bucket `{bucket}`:**
```
{bucket}/
 ├── raw/
 │   ├── ordenes_csv/          ← ordenes_raw_ampliado.csv
 │   └── eventos_json/         ← eventos_clinicos_ampliado.jsonl
 ├── athena-results/           ← resultados de Athena
 └── curated/
     └── ordenes_parquet/      ← tabla curada (Parquet)
```
""")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 · TABLAS EXTERNAS
# ─────────────────────────────────────────────────────────────────────────────
with tabs[1]:
    st.header("Crear tablas externas en Athena")

    sql_csv = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database}.ordenes_raw_csv (
    orden_id            STRING,
    fecha               STRING,
    glosa               STRING,
    medio_emisor        STRING,
    valor_procedimiento DOUBLE,
    entidad_emisora     STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\\"",
    "escapeChar"    = "\\\\"
)
LOCATION 's3://{bucket}/raw/ordenes_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');"""

    sql_json = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database}.eventos_clinicos_json (
    orden_id     STRING,
    paciente     STRUCT<
                     nombre:STRING,
                     dno:STRING,
                     seguro_social:STRING
                 >,
    edad         INT,
    medicamentos ARRAY<STRING>,
    diagnostico  STRING,
    ciudad       STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{bucket}/raw/eventos_json/';"""

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Tabla CSV – órdenes")
        st.markdown(f'<div class="sql-box">{sql_csv}</div>', unsafe_allow_html=True)
        if st.button("▶ Crear tabla CSV", key="btn_tbl_csv"):
            err = ddl_run(athena, sql_csv, output)
            if err:
                st.error(f"Error: {err}")
            else:
                st.success("Tabla `ordenes_raw_csv` creada.")

    with col2:
        st.markdown("### Tabla JSON – eventos clínicos")
        st.markdown(f'<div class="sql-box">{sql_json}</div>', unsafe_allow_html=True)
        if st.button("▶ Crear tabla JSON", key="btn_tbl_json"):
            err = ddl_run(athena, sql_json, output)
            if err:
                st.error(f"Error: {err}")
            else:
                st.success("Tabla `eventos_clinicos_json` creada.")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 · VERIFICACIÓN
# ─────────────────────────────────────────────────────────────────────────────
with tabs[2]:
    st.header("Verificación de tablas")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("COUNT(*) – CSV")
        sql_cnt_csv = f"SELECT COUNT(*) AS total_ordenes FROM {database}.ordenes_raw_csv;"
        st.markdown(f'<div class="sql-box">{sql_cnt_csv}</div>', unsafe_allow_html=True)
        if st.button("▶ Verificar CSV", key="btn_ver_csv"):
            df, err = run_query(athena, sql_cnt_csv, database, output)
            if err:
                st.error(err)
            else:
                total = int(df.iloc[0, 0]) if not df.empty else 0
                if total > 0:
                    st.success(f"✅ {total:,} órdenes encontradas")
                else:
                    st.warning("⚠️ La tabla devolvió 0 filas. Revisa la ruta LOCATION.")
                st.dataframe(df, use_container_width=True)

    with col2:
        st.subheader("Preview – JSON")
        sql_prev_json = (
            f"SELECT orden_id, paciente.nombre, diagnostico "
            f"FROM {database}.eventos_clinicos_json LIMIT 10;"
        )
        st.markdown(f'<div class="sql-box">{sql_prev_json}</div>', unsafe_allow_html=True)
        if st.button("▶ Verificar JSON", key="btn_ver_json"):
            df, err = run_query(athena, sql_prev_json, database, output)
            if err:
                st.error(err)
            else:
                if df.empty:
                    st.warning("⚠️ Sin datos. Revisa el archivo JSONL (1 objeto por línea).")
                else:
                    st.success(f"✅ {len(df)} filas de preview")
                    st.dataframe(df, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 · CONSULTAS PRINCIPALES
# ─────────────────────────────────────────────────────────────────────────────
with tabs[3]:
    st.header("Consultas principales del ejercicio")

    QUERIES = {
        "12.1 · Promedio por entidad emisora": {
            "sql": (
                f"SELECT entidad_emisora,\n"
                f"       AVG(valor_procedimiento) AS promedio_valor\n"
                f"FROM {database}.ordenes_raw_csv\n"
                f"GROUP BY entidad_emisora\n"
                f"ORDER BY promedio_valor DESC;"
            ),
            "badge": "raw",
            "chart": "bar",
            "x": "entidad_emisora",
            "y": "promedio_valor",
        },
        "12.2 · Cruce órdenes ↔ eventos (JOIN)": {
            "sql": (
                f"SELECT o.orden_id, o.fecha, o.entidad_emisora,\n"
                f"       e.paciente.nombre AS nombre_paciente,\n"
                f"       e.diagnostico\n"
                f"FROM {database}.ordenes_raw_csv o\n"
                f"JOIN {database}.eventos_clinicos_json e\n"
                f"  ON o.orden_id = e.orden_id\n"
                f"LIMIT 20;"
            ),
            "badge": "raw",
            "chart": None,
        },
        "12.3 · UNNEST medicamentos (preview)": {
            "sql": (
                f"SELECT e.orden_id, medicamento\n"
                f"FROM {database}.eventos_clinicos_json e\n"
                f"CROSS JOIN UNNEST(e.medicamentos) AS t(medicamento)\n"
                f"LIMIT 20;"
            ),
            "badge": "raw",
            "chart": None,
        },
        "12.4 · Frecuencia de medicamentos": {
            "sql": (
                f"SELECT medicamento, COUNT(*) AS frecuencia\n"
                f"FROM {database}.eventos_clinicos_json e\n"
                f"CROSS JOIN UNNEST(e.medicamentos) AS t(medicamento)\n"
                f"GROUP BY medicamento\n"
                f"ORDER BY frecuencia DESC;"
            ),
            "badge": "raw",
            "chart": "bar",
            "x": "medicamento",
            "y": "frecuencia",
        },
    }

    for title, cfg in QUERIES.items():
        badge_html = (
            f'<span class="badge badge-{cfg["badge"]}">{cfg["badge"].upper()}</span>'
        )
        st.markdown(f"### {title} {badge_html}", unsafe_allow_html=True)
        st.markdown(f'<div class="sql-box">{cfg["sql"]}</div>', unsafe_allow_html=True)

        btn_key = f"btn_{title[:10].replace(' ', '_')}"
        if st.button(f"▶ Ejecutar", key=btn_key):
            df, err = run_query(athena, cfg["sql"], database, output)
            if err:
                st.error(err)
            else:
                st.success(f"{len(df)} filas devueltas")
                if cfg.get("chart") == "bar" and not df.empty:
                    try:
                        df_chart = df.copy()
                        df_chart[cfg["y"]] = pd.to_numeric(df_chart[cfg["y"]], errors="coerce")
                        st.bar_chart(df_chart.set_index(cfg["x"])[cfg["y"]])
                    except Exception:
                        pass
                st.dataframe(df, use_container_width=True)

        st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 · CAPA CURADA (PARQUET / CTAS)
# ─────────────────────────────────────────────────────────────────────────────
with tabs[4]:
    st.header("Capa curada – Parquet (CTAS)")

    sql_ctas = f"""CREATE TABLE {database}.ordenes_curadas_parquet
WITH (
    format            = 'PARQUET',
    external_location = 's3://{bucket}/curated/ordenes_parquet/'
) AS
SELECT
    o.orden_id,
    CAST(date_parse(o.fecha, '%Y-%m-%d') AS DATE) AS fecha,
    o.glosa,
    o.medio_emisor,
    o.valor_procedimiento,
    o.entidad_emisora,
    e.paciente.nombre        AS paciente_nombre,
    e.paciente.dno           AS paciente_dno,
    e.paciente.seguro_social AS seguro_social,
    e.edad,
    e.medicamentos,
    e.diagnostico,
    e.ciudad
FROM {database}.ordenes_raw_csv o
JOIN {database}.eventos_clinicos_json e
  ON o.orden_id = e.orden_id;"""

    sql_val = f"""SELECT diagnostico,
       COUNT(*) AS casos,
       ROUND(AVG(valor_procedimiento), 2) AS costo_promedio
FROM {database}.ordenes_curadas_parquet
GROUP BY diagnostico
ORDER BY casos DESC;"""

    col1, col2 = st.columns([3, 2])

    with col1:
        st.subheader("CTAS – crear tabla curada")
        st.markdown(f'<div class="sql-box">{sql_ctas}</div>', unsafe_allow_html=True)
        st.warning(
            "⚠️ La carpeta `curated/ordenes_parquet/` debe estar vacía antes de ejecutar. "
            "Si ya existe la tabla, primero elimínala con DROP TABLE."
        )

        drop_first = st.checkbox("DROP TABLE antes de crear (si ya existe)", key="chk_drop")

        if st.button("▶ Crear capa curada (Parquet)", key="btn_ctas"):
            if drop_first:
                sql_drop = f"DROP TABLE IF EXISTS {database}.ordenes_curadas_parquet;"
                err_drop = ddl_run(athena, sql_drop, output)
                if err_drop:
                    st.error(f"DROP TABLE falló: {err_drop}")
                    st.stop()
                else:
                    st.info("Tabla anterior eliminada.")
            err = ddl_run(athena, sql_ctas, output)
            if err:
                st.error(f"Error en CTAS: {err}")
            else:
                st.success(
                    "✅ Tabla `ordenes_curadas_parquet` creada. "
                    f"Revisa la carpeta `s3://{bucket}/curated/ordenes_parquet/` en S3."
                )

    with col2:
        st.subheader("Validación sobre la capa curada")
        st.markdown(f'<div class="sql-box">{sql_val}</div>', unsafe_allow_html=True)
        if st.button("▶ Validar capa curada", key="btn_val_curada"):
            df, err = run_query(athena, sql_val, database, output)
            if err:
                st.error(err)
            else:
                st.success(f"{len(df)} diagnósticos encontrados")
                if not df.empty:
                    df["casos"]          = pd.to_numeric(df["casos"], errors="coerce")
                    df["costo_promedio"] = pd.to_numeric(df["costo_promedio"], errors="coerce")
                    st.bar_chart(df.set_index("diagnostico")["casos"])
                st.dataframe(df, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 6 · SQL LIBRE
# ─────────────────────────────────────────────────────────────────────────────
with tabs[5]:
    st.header("Editor SQL libre")
    st.caption("Escribe cualquier consulta Athena y ejecútala directamente.")

    sql_libre = st.text_area(
        "SQL",
        height=200,
        placeholder=f"SELECT * FROM {database}.ordenes_raw_csv LIMIT 5;",
        key="sql_libre",
    )

    col_db, col_btn = st.columns([3, 1])
    with col_db:
        db_override = st.text_input("Base de datos", value=database, key="db_override")
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        run_libre = st.button("▶ Ejecutar SQL", key="btn_libre", use_container_width=True)

    if run_libre:
        if not sql_libre.strip():
            st.warning("Escribe una consulta primero.")
        else:
            df, err = run_query(athena, sql_libre, db_override, output)
            if err:
                st.error(f"Error Athena: {err}")
            else:
                st.success(f"{len(df)} filas")
                st.dataframe(df, use_container_width=True)
                csv_data = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Descargar resultado CSV",
                    data=csv_data,
                    file_name="resultado_athena.csv",
                    mime="text/csv",
                )

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Ejercicio Big Data · Amazon S3 + Amazon Athena · "
    "Construido con Streamlit & boto3"
)
