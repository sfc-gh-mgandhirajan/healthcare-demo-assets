import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import base64
import requests
import os
from datetime import timedelta
from streamlit_floating_container import FloatingContainer

IS_SIS = os.getenv("SNOWFLAKE_HOST") is not None

st.set_page_config(
    page_title="Snowflake DICOM Explorer",
    page_icon=":material/radiology:",
    layout="wide",
)

# ---------------------------------------------------------------------------
# SNOWFLAKE BRAND THEME
# ---------------------------------------------------------------------------
SF_BLUE = "#29B5E8"
SF_BLUE_DARK = "#11567F"
SF_NAVY = "#1B2A4A"
SF_GRAY_DARK = "#2D3748"
SF_GRAY_MED = "#718096"
SF_GRAY_LIGHT = "#E2E8F0"
SF_BG = "#F4F8FB"
SF_WHITE = "#FFFFFF"
SF_TEAL = "#38B2AC"
SF_INDIGO = "#5A67D8"
SF_GREEN = "#48BB78"

PALETTE = [SF_BLUE, SF_INDIGO, SF_TEAL, SF_GREEN, SF_NAVY, SF_GRAY_MED, SF_BLUE_DARK, "#ED8936"]

PLOTLY_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=40, b=40),
    font=dict(size=12, color=SF_NAVY, family="Inter, -apple-system, sans-serif"),
    height=380,
    colorway=PALETTE,
)

st.markdown(f"""
<style>
    [data-testid="stSidebar"] {{
        background-color: {SF_NAVY};
    }}
    [data-testid="stSidebar"] * {{
        color: {SF_WHITE} !important;
    }}
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown span {{
        color: {SF_WHITE} !important;
    }}
    [data-testid="stSidebar"] hr {{
        border-color: rgba(255,255,255,0.15);
    }}
    div[data-baseweb="tab-highlight"] {{
        background-color: {SF_BLUE} !important;
    }}
    .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {{
        color: {SF_BLUE} !important;
    }}
    .stMetricValue {{
        color: {SF_BLUE_DARK} !important;
    }}
    button[kind="primary"] {{
        background-color: {SF_BLUE} !important;
        border-color: {SF_BLUE} !important;
    }}
    button[kind="primary"]:hover {{
        background-color: {SF_BLUE_DARK} !important;
        border-color: {SF_BLUE_DARK} !important;
    }}
    .sf-header {{
        background: linear-gradient(135deg, {SF_NAVY} 0%, {SF_BLUE_DARK} 100%);
        padding: 1rem 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }}
    .sf-header h2 {{
        color: white !important;
        margin: 0 !important;
        font-size: 1.3rem !important;
    }}
    .sf-header p {{
        color: rgba(255,255,255,0.75) !important;
        margin: 0.2rem 0 0 0 !important;
        font-size: 0.85rem !important;
    }}
</style>
""", unsafe_allow_html=True)


if IS_SIS:
    @st.cache_resource
    def get_connection():
        return st.connection("snowflake")

    def run_query(sql):
        conn = get_connection()
        return conn.query(sql)
else:
    import snowflake.connector

    @st.cache_resource
    def get_connection():
        conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "spark-connect"
        return snowflake.connector.connect(connection_name=conn_name)

    def run_query(sql):
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
        finally:
            cur.close()


@st.cache_data(ttl=timedelta(minutes=30))
def load_metadata():
    return run_query("""
        SELECT FILE_NAME, FILE_SIZE, PATIENT_ID, PATIENT_NAME, PATIENT_SEX, PATIENT_AGE,
               STUDY_INSTANCE_UID, STUDY_DATE, STUDY_DESCRIPTION, SERIES_INSTANCE_UID,
               SERIES_NUMBER, SERIES_DESCRIPTION, MODALITY, MANUFACTURER, INSTITUTION_NAME,
               BODY_PART_EXAMINED, IMAGE_ROWS, IMAGE_COLUMNS, SLICE_THICKNESS, SLICE_LOCATION,
               WINDOW_CENTER, WINDOW_WIDTH, INSTANCE_NUMBER, KVP, EXPOSURE,
               CONVOLUTION_KERNEL, PROTOCOL_NAME, COLLECTION_NAME, SERIES_LABEL
        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
    """)


@st.cache_data(ttl=timedelta(minutes=30))
def load_series_summary():
    return run_query("""
        SELECT COLLECTION_NAME, SERIES_LABEL, MODALITY,
               COUNT(*) AS FILE_COUNT,
               COUNT(DISTINCT PATIENT_ID) AS PATIENTS,
               ANY_VALUE(MANUFACTURER) AS MANUFACTURER,
               ANY_VALUE(BODY_PART_EXAMINED) AS BODY_PART,
               ROUND(SUM(FILE_SIZE)/1024/1024, 1) AS SIZE_MB,
               ANY_VALUE(STUDY_DESCRIPTION) AS STUDY_DESCRIPTION,
               ANY_VALUE(SERIES_DESCRIPTION) AS SERIES_DESCRIPTION,
               ANY_VALUE(PROTOCOL_NAME) AS PROTOCOL_NAME,
               MIN(INSTANCE_NUMBER::INT) AS MIN_INSTANCE,
               MAX(INSTANCE_NUMBER::INT) AS MAX_INSTANCE
        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
        GROUP BY COLLECTION_NAME, SERIES_LABEL, MODALITY
        ORDER BY FILE_COUNT DESC
    """)


def fmt(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:,.0f}"


def sf_section_header(title, subtitle=None):
    sub_html = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f'<div class="sf-header"><h2>{title}</h2>{sub_html}</div>', unsafe_allow_html=True)


def _clean_response(text):
    return text.replace("\\n", "\n").replace("\\t", "\t")


# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(f"""
    <div style="text-align:center; padding: 0.75rem 0 1rem 0;">
        <div style="font-size: 1.1rem; font-weight: 600; letter-spacing: 0.5px; color: {SF_WHITE} !important; line-height: 1.3;">
            &#10052; DICOM Explorer
        </div>
        <div style="font-size: 0.65rem; letter-spacing: 2px; text-transform: uppercase; color: {SF_BLUE} !important; margin-top: 4px;">
            Snowflake
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.divider()
    st.markdown(f"""
    <div style="padding: 0 0.5rem;">
        <div style="font-size: 0.75rem; color: {SF_GRAY_LIGHT} !important; line-height: 1.7;">
            <span style="color: {SF_BLUE} !important;">&#9679;</span> 10 CT series &bull; 5 IDC collections<br/>
            <span style="color: {SF_BLUE} !important;">&#9679;</span> 4 manufacturers &bull; 2,848 files<br/>
            <span style="color: {SF_BLUE} !important;">&#9679;</span> EW_IMAGING_DB.EXPLORER
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.divider()
    st.markdown(f"""
    <div style="font-size: 0.6rem; color: {SF_GRAY_MED} !important; padding: 0 0.5rem; line-height: 1.6;">
        Cortex AI &bull; Python UDFs &bull; Streamlit
    </div>
    """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# TABS
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab7, tab6 = st.tabs([
    ":material/account_tree: Pipeline",
    ":material/search: Explorer",
    ":material/radiology: Viewer",
    ":material/neurology: AI Insights",
    ":material/ecg_heart: Clinical Intelligence",
    ":material/image_search: Image Search",
    ":material/shield: Governance",
])


# =========================================================================
# TAB 1: PIPELINE OVERVIEW
# =========================================================================
with tab1:
    df = load_metadata()
    summary = load_series_summary()

    sf_section_header("Pipeline Overview", "End-to-end DICOM ingestion from S3 to structured analytics")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        with st.container(border=True):
            st.metric("Total DICOM Files", fmt(len(df)))
    with c2:
        with st.container(border=True):
            st.metric("Studies", fmt(df["STUDY_INSTANCE_UID"].nunique()))
    with c3:
        with st.container(border=True):
            st.metric("Patients", fmt(df["PATIENT_ID"].nunique()))
    with c4:
        with st.container(border=True):
            st.metric("Collections", fmt(df["COLLECTION_NAME"].nunique()))
    with c5:
        with st.container(border=True):
            st.metric("Total Size", f"{df['FILE_SIZE'].sum()/1024/1024:.0f} MB")

    col_a, col_b = st.columns(2)
    with col_a:
        with st.container(border=True):
            fig = px.bar(
                summary, x="SERIES_LABEL", y="FILE_COUNT", color="COLLECTION_NAME",
                color_discrete_sequence=PALETTE, title="Files by Series",
            )
            fig.update_layout(**PLOTLY_LAYOUT, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        with st.container(border=True):
            mfr_counts = df.groupby("MANUFACTURER").size().reset_index(name="COUNT")
            fig2 = px.pie(mfr_counts, names="MANUFACTURER", values="COUNT",
                          color_discrete_sequence=PALETTE, title="Files by Manufacturer")
            fig2.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

    with st.container(border=True):
        st.subheader("Series Detail", anchor=False)
        st.dataframe(
            summary[["COLLECTION_NAME", "SERIES_LABEL", "MODALITY", "MANUFACTURER",
                      "FILE_COUNT", "SIZE_MB", "STUDY_DESCRIPTION", "SERIES_DESCRIPTION"]],
            hide_index=True, use_container_width=True,
        )

    with st.expander("How it works: Python UDF on DICOM binary"):
        st.code("""
CREATE OR REPLACE FUNCTION EXTRACT_DICOM_METADATA(file_path STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pydicom')
HANDLER = 'extract_metadata'
AS $$
import pydicom, io
from snowflake.snowpark.files import SnowflakeFile

def extract_metadata(file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
        ds = pydicom.dcmread(io.BytesIO(f.read()))
    result = {}
    for elem in ds:
        if elem.VR not in ('OW','OB','OF','OD','UN','SQ'):
            result[elem.keyword.upper()] = str(elem.value)
    return result
$$""", language="sql")


# =========================================================================
# TAB 2: METADATA EXPLORER
# =========================================================================
with tab2:
    df = load_metadata()

    sf_section_header("Metadata Explorer", "Search and filter across 41 DICOM metadata attributes")

    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        sel_collection = st.multiselect("Collection", df["COLLECTION_NAME"].unique(), default=[], placeholder="All collections")
    with fc2:
        sel_manufacturer = st.multiselect("Manufacturer", df["MANUFACTURER"].dropna().unique(), default=[], placeholder="All manufacturers")
    with fc3:
        sel_body = st.multiselect("Body Part", df["BODY_PART_EXAMINED"].dropna().unique(), default=[], placeholder="All")
    with fc4:
        sel_modality = st.multiselect("Modality", df["MODALITY"].dropna().unique(), default=[], placeholder="All")

    filtered = df.copy()
    if sel_collection:
        filtered = filtered[filtered["COLLECTION_NAME"].isin(sel_collection)]
    if sel_manufacturer:
        filtered = filtered[filtered["MANUFACTURER"].isin(sel_manufacturer)]
    if sel_body:
        filtered = filtered[filtered["BODY_PART_EXAMINED"].isin(sel_body)]
    if sel_modality:
        filtered = filtered[filtered["MODALITY"].isin(sel_modality)]

    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        with st.container(border=True):
            st.metric("Filtered Files", fmt(len(filtered)))
    with mc2:
        with st.container(border=True):
            st.metric("Studies", fmt(filtered["STUDY_INSTANCE_UID"].nunique()))
    with mc3:
        with st.container(border=True):
            st.metric("Patients", fmt(filtered["PATIENT_ID"].nunique()))
    with mc4:
        with st.container(border=True):
            st.metric("Avg Slice Thickness", f"{pd.to_numeric(filtered['SLICE_THICKNESS'], errors='coerce').mean():.2f} mm")

    with st.container(border=True):
        display_cols = ["SERIES_LABEL", "COLLECTION_NAME", "PATIENT_ID", "MODALITY",
                        "MANUFACTURER", "STUDY_DESCRIPTION", "SERIES_DESCRIPTION",
                        "SLICE_THICKNESS", "KVP", "EXPOSURE", "PROTOCOL_NAME"]
        study_agg = filtered.groupby("SERIES_LABEL").agg(
            COLLECTION_NAME=("COLLECTION_NAME", "first"),
            PATIENT_ID=("PATIENT_ID", "first"),
            MODALITY=("MODALITY", "first"),
            MANUFACTURER=("MANUFACTURER", "first"),
            STUDY_DESCRIPTION=("STUDY_DESCRIPTION", "first"),
            SERIES_DESCRIPTION=("SERIES_DESCRIPTION", "first"),
            FILES=("FILE_NAME", "count"),
            AVG_SLICE_THICKNESS=("SLICE_THICKNESS", lambda x: pd.to_numeric(x, errors="coerce").mean()),
            KVP=("KVP", "first"),
            PROTOCOL_NAME=("PROTOCOL_NAME", "first"),
        ).reset_index()
        st.dataframe(study_agg, hide_index=True, use_container_width=True)

    with st.expander("Raw DICOM Metadata (JSON)"):
        sel_series_json = st.selectbox("Select series", filtered["SERIES_LABEL"].unique(), key="json_series")
        sample_row = run_query(f"""
            SELECT RAW_METADATA FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
            WHERE SERIES_LABEL = '{sel_series_json}' LIMIT 1
        """)
        if not sample_row.empty:
            raw = sample_row.iloc[0]["RAW_METADATA"]
            if isinstance(raw, str):
                raw = json.loads(raw)
            st.json(raw, expanded=False)


# =========================================================================
# TAB 3: DICOM VIEWER
# =========================================================================
with tab3:
    summary = load_series_summary()

    sf_section_header("DICOM Viewer", "Real-time medical image rendering via Snowflake Python UDF")

    vc1, vc2 = st.columns([1, 3])
    with vc1:
        series_options = summary["SERIES_LABEL"].tolist()
        sel_series = st.selectbox("Series", series_options, key="viewer_series")
        row = summary[summary["SERIES_LABEL"] == sel_series].iloc[0]

        min_inst = int(row["MIN_INSTANCE"]) if pd.notna(row["MIN_INSTANCE"]) else 1
        max_inst = int(row["MAX_INSTANCE"]) if pd.notna(row["MAX_INSTANCE"]) else 100
        slice_num = st.slider("Slice", min_inst, max_inst, (min_inst + max_inst) // 2, key="slice_slider")

        preset = st.selectbox("Window Preset", ["Soft Tissue", "Lung", "Bone", "Mediastinum", "Custom"], key="preset")
        presets = {
            "Soft Tissue": (40, 400),
            "Lung": (-600, 1500),
            "Bone": (300, 1500),
            "Mediastinum": (50, 350),
        }
        if preset == "Custom":
            wc = st.number_input("Window Center", value=40, key="custom_wc")
            ww = st.number_input("Window Width", value=400, key="custom_ww")
        else:
            wc, ww = presets[preset]

        st.divider()
        st.markdown(f"**Collection:** {row['COLLECTION_NAME']}")
        st.markdown(f"**Manufacturer:** {row['MANUFACTURER']}")
        st.markdown(f"**Description:** {row['SERIES_DESCRIPTION']}")
        st.markdown(f"**Slices:** {row['FILE_COUNT']}")

    with vc2:
        @st.cache_data(ttl=timedelta(hours=1))
        def render_slice(_series_label, instance_num, w_center, w_width):
            result = run_query(f"""
                SELECT EW_IMAGING_DB.EXPLORER.RENDER_DICOM_SLICE(
                    BUILD_SCOPED_FILE_URL(@EW_IMAGING_DB.EXPLORER.IDC_OPEN_DATA_ALL_STG, FILE_NAME),
                    {w_center}, {w_width}
                ) AS IMG
                FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
                WHERE SERIES_LABEL = '{_series_label}'
                  AND INSTANCE_NUMBER = '{instance_num}'
                LIMIT 1
            """)
            if not result.empty:
                return result.iloc[0]["IMG"]
            return None

        with st.spinner("Rendering DICOM slice..."):
            img_b64 = render_slice(sel_series, slice_num, wc, ww)

        if img_b64 and not img_b64.startswith("ERROR"):
            img_bytes = base64.b64decode(img_b64)
            st.image(img_bytes, caption=f"Slice {slice_num} | W/L: {wc}/{ww}", use_container_width=True)
        elif img_b64 and img_b64.startswith("ERROR"):
            st.error(f"Render error: {img_b64}")
        else:
            st.warning("No image found for this slice number. Try a different slice.")


# =========================================================================
# TAB 4: AI INSIGHTS
# =========================================================================
with tab4:
    sf_section_header("AI Insights", "Cortex AI classification, summarization, and anomaly detection")

    ai_col1, ai_col2 = st.columns(2)
    with ai_col1:
        with st.container(border=True):
            st.markdown(f"##### :material/category: Study Classification")
            st.caption("Uses AI_CLASSIFY to categorize each series by clinical type")
            if st.button("Classify Studies", key="btn_classify", type="primary"):
                with st.spinner("Running AI_CLASSIFY..."):
                    classify_result = run_query("""
                        SELECT SERIES_LABEL,
                            AI_CLASSIFY(
                                COALESCE(STUDY_DESCRIPTION,'') || ' ' || COALESCE(SERIES_DESCRIPTION,'') || ' ' || COALESCE(PROTOCOL_NAME,'') || ' ' || MODALITY,
                                ['Cardiac CT Angiography', 'Cardiac Gated CT', 'Chest CT Screening', 'CT Attenuation Correction', 'Coronary Calcium Score']
                            ) AS CLASSIFICATION
                        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
                        GROUP BY SERIES_LABEL, STUDY_DESCRIPTION, SERIES_DESCRIPTION, PROTOCOL_NAME, MODALITY
                    """)
                    st.dataframe(classify_result, hide_index=True, use_container_width=True)

    with ai_col2:
        with st.container(border=True):
            st.markdown(f"##### :material/summarize: Clinical Summary")
            st.caption("Uses AI_COMPLETE (Claude Sonnet) to generate a clinical summary")
            sel_series_ai = st.selectbox("Select series", load_series_summary()["SERIES_LABEL"].tolist(), key="ai_series")
            if st.button("Generate Summary", key="btn_summary", type="primary"):
                with st.spinner("Generating clinical summary..."):
                    summary_result = run_query(f"""
                        SELECT AI_COMPLETE('claude-4-sonnet',
                            'You are a radiology informatics specialist. Given this DICOM metadata for a cardiac imaging study, generate a concise 3-sentence clinical summary. Include modality, acquisition parameters, and notable metadata findings.\n\nMetadata: ' ||
                            ANY_VALUE(RAW_METADATA)::STRING
                        ) AS SUMMARY
                        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
                        WHERE SERIES_LABEL = '{sel_series_ai}'
                        GROUP BY SERIES_LABEL
                    """)
                    if not summary_result.empty:
                        summary_text = _clean_response(summary_result.iloc[0]["SUMMARY"])
                        st.info(summary_text)

    with st.container(border=True):
        st.markdown("##### :material/monitoring: Parameter Anomaly Detection")
        st.caption("Distribution analysis to identify acquisition protocol deviations")
        df = load_metadata()
        kvp_vals = pd.to_numeric(df["KVP"], errors="coerce").dropna()
        exposure_vals = pd.to_numeric(df["EXPOSURE"], errors="coerce").dropna()
        thickness_vals = pd.to_numeric(df["SLICE_THICKNESS"], errors="coerce").dropna()

        an1, an2, an3 = st.columns(3)
        with an1:
            fig_kvp = px.histogram(kvp_vals, nbins=20, title="KVP Distribution",
                                   color_discrete_sequence=[SF_BLUE])
            fig_kvp.update_layout(**{**PLOTLY_LAYOUT, 'height': 280})
            st.plotly_chart(fig_kvp, use_container_width=True)
        with an2:
            fig_exp = px.histogram(exposure_vals, nbins=30, title="Exposure Distribution",
                                   color_discrete_sequence=[SF_TEAL])
            fig_exp.update_layout(**{**PLOTLY_LAYOUT, 'height': 280})
            st.plotly_chart(fig_exp, use_container_width=True)
        with an3:
            fig_th = px.histogram(thickness_vals, nbins=20, title="Slice Thickness Distribution",
                                  color_discrete_sequence=[SF_INDIGO])
            fig_th.update_layout(**{**PLOTLY_LAYOUT, 'height': 280})
            st.plotly_chart(fig_th, use_container_width=True)


# =========================================================================
# TAB 5: CLINICAL INTELLIGENCE
# =========================================================================
with tab5:
    sf_section_header("Clinical Intelligence", "TAVR procedures, device telemetry, adverse events, and CAPA — joined to imaging")

    proc_df = run_query("SELECT * FROM EW_IMAGING_DB.EXPLORER.FACT_PROCEDURE")
    ae_df = run_query("SELECT * FROM EW_IMAGING_DB.EXPLORER.FACT_ADVERSE_EVENT")
    capa_df = run_query("SELECT * FROM EW_IMAGING_DB.EXPLORER.FACT_CAPA")
    tele_df = run_query("""
        SELECT t.*, d.MODEL_NAME
        FROM EW_IMAGING_DB.EXPLORER.FACT_DEVICE_TELEMETRY t
        JOIN EW_IMAGING_DB.EXPLORER.DIM_DEVICE d ON t.DEVICE_ID = d.DEVICE_ID
    """)

    ck1, ck2, ck3, ck4, ck5 = st.columns(5)
    with ck1:
        with st.container(border=True):
            st.metric("TAVR Procedures", len(proc_df))
    with ck2:
        with st.container(border=True):
            st.metric("Avg LOS (days)", f"{proc_df['LOS_DAYS'].mean():.1f}")
    with ck3:
        with st.container(border=True):
            st.metric("Adverse Events", len(ae_df))
    with ck4:
        with st.container(border=True):
            st.metric("Open CAPAs", len(capa_df[capa_df['STATUS'] == 'Open']))
    with ck5:
        with st.container(border=True):
            st.metric("Device Models", run_query("SELECT COUNT(DISTINCT MODEL_NAME) AS C FROM EW_IMAGING_DB.EXPLORER.DIM_DEVICE").iloc[0]["C"])

    cl1, cl2 = st.columns(2)
    with cl1:
        with st.container(border=True):
            outcome_by_route = proc_df.groupby(["ACCESS_ROUTE", "OUTCOME"]).size().reset_index(name="COUNT")
            fig_out = px.bar(outcome_by_route, x="ACCESS_ROUTE", y="COUNT", color="OUTCOME",
                             barmode="group", color_discrete_sequence=[SF_GREEN, SF_BLUE],
                             title="Procedure Outcomes by Access Route")
            fig_out.update_layout(**{**PLOTLY_LAYOUT, 'height': 340})
            st.plotly_chart(fig_out, use_container_width=True)

    with cl2:
        with st.container(border=True):
            ae_summary = ae_df.groupby(["EVENT_TYPE", "SEVERITY"]).size().reset_index(name="COUNT")
            fig_ae = px.bar(ae_summary, x="EVENT_TYPE", y="COUNT", color="SEVERITY",
                            barmode="group", color_discrete_sequence=[SF_TEAL, SF_BLUE, SF_NAVY],
                            title="Adverse Events by Type & Severity")
            fig_ae.update_layout(**{**PLOTLY_LAYOUT, 'height': 340}, xaxis_tickangle=-30)
            st.plotly_chart(fig_ae, use_container_width=True)

    with st.container(border=True):
        st.markdown("##### :material/monitoring: Post-Implant Hemodynamic Telemetry")
        st.caption("Mean pressure gradient over time — elevated early post-op, stabilizing over months")
        tele_df["READING_DATE"] = pd.to_datetime(tele_df["READING_DATE"])
        fig_tele = px.line(tele_df, x="READING_DATE", y="MEAN_GRADIENT_MMHG",
                           color="PATIENT_ID", title="Mean Gradient Over Time by Patient",
                           color_discrete_sequence=PALETTE)
        fig_tele.update_layout(**{**PLOTLY_LAYOUT, 'height': 380})
        st.plotly_chart(fig_tele, use_container_width=True)

    with st.container(border=True):
        st.markdown("##### :material/table: Procedure Detail — Imaging to Outcome")
        proc_detail = run_query("""
            SELECT p.PROCEDURE_ID, p.PATIENT_ID, d.MODEL_NAME, d.VALVE_SIZE_MM,
                   s.SITE_NAME, s.IMPLANT_VOLUME_TIER,
                   p.ACCESS_ROUTE, p.PROCEDURE_DATE, p.OUTCOME, p.LOS_DAYS,
                   m.SERIES_LABEL, m.MANUFACTURER AS SCANNER_MFR
            FROM EW_IMAGING_DB.EXPLORER.FACT_PROCEDURE p
            JOIN EW_IMAGING_DB.EXPLORER.DIM_DEVICE d ON p.DEVICE_ID = d.DEVICE_ID
            JOIN EW_IMAGING_DB.EXPLORER.DIM_SITE s ON p.SITE_ID = s.SITE_ID
            LEFT JOIN (
                SELECT DISTINCT PATIENT_ID, SERIES_LABEL, MANUFACTURER
                FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
            ) m ON p.PATIENT_ID = m.PATIENT_ID
            ORDER BY p.PROCEDURE_DATE
        """)
        st.dataframe(proc_detail, hide_index=True, use_container_width=True)


# =========================================================================
# TAB 7: IMAGE SEARCH (MedSigLIP Vision-Language)
# =========================================================================
with tab7:
    sf_section_header("Image Search", "Semantic search over DICOM images using MedSigLIP vision-language embeddings")

    if "image_search_results" not in st.session_state:
        st.session_state.image_search_results = None
    if "image_search_query" not in st.session_state:
        st.session_state.image_search_query = ""

    def _run_image_search(query):
        st.session_state.image_search_query = query

    with st.container(border=True):
        st.markdown("##### :material/image_search: Describe what you're looking for")
        st.caption("Natural language queries are encoded into the same 1152-dim vector space as the DICOM images")

        search_input = st.text_input(
            "Search query",
            placeholder="e.g., cardiac CT angiography with aortic valve calcification",
            key="image_search_input",
            label_visibility="collapsed",
        )

        example_prompts = [
            "Cardiac CTA with aortic valve calcification",
            "Contrast-enhanced coronary angiography",
            "Chest CT lung parenchyma",
            "ECG-gated diastolic cardiac CT",
        ]
        ecols = st.columns(4)
        for i, prompt in enumerate(example_prompts):
            ecols[i].button(
                prompt, key=f"img_search_{i}", use_container_width=True,
                on_click=_run_image_search, args=(prompt,),
            )

    active_query = search_input if search_input else st.session_state.image_search_query
    if active_query:
        with st.spinner(f"Searching for: *{active_query}*"):
            safe_q = active_query.replace("'", "''")
            search_results = run_query(f"""
                CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('{safe_q}', 9)
            """)

            if not search_results.empty:
                raw_variant = search_results.iloc[0][search_results.columns[0]]
                if isinstance(raw_variant, str):
                    results_list = json.loads(raw_variant)
                else:
                    results_list = raw_variant
            else:
                results_list = []

        if results_list:
            # Deduplicate to one result per series
            seen_series = {}
            for r in results_list:
                series_id = r.get("SERIES_UUID", "")
                if series_id not in seen_series:
                    seen_series[series_id] = r
            unique_results = list(seen_series.values())[:6]

            st.markdown(f"**{len(results_list)} matches** across **{len(unique_results)} series** for: *{active_query}*")

            @st.cache_data(ttl=timedelta(hours=1))
            def render_series_middle_slice(series_uuid):
                """Render the middle slice of a series for a representative preview."""
                r = run_query(f"""
                    WITH ordered AS (
                        SELECT FILE_NAME,
                               ROW_NUMBER() OVER (ORDER BY INSTANCE_NUMBER) AS RN,
                               COUNT(*) OVER () AS TOTAL
                        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
                        WHERE SPLIT_PART(FILE_NAME, '/', 1) = '{series_uuid}'
                    )
                    SELECT EW_IMAGING_DB.EXPLORER.RENDER_DICOM_SLICE(
                        BUILD_SCOPED_FILE_URL(@EW_IMAGING_DB.EXPLORER.IDC_OPEN_DATA_ALL_STG, FILE_NAME),
                        40, 400
                    ) AS IMG
                    FROM ordered
                    WHERE RN = FLOOR(TOTAL / 2) + 1
                """)
                if not r.empty:
                    return r.iloc[0]["IMG"]
                return None

            # Render image grid (one per series)
            grid_cols = st.columns(3)
            for idx, result in enumerate(unique_results):
                col = grid_cols[idx % 3]
                series_uuid = result.get("SERIES_UUID", "")
                label = result.get("LABEL", "Unknown")
                collection = result.get("COLLECTION", "")

                with col:
                    with st.container(border=True):
                        st.markdown(f"**{label}**")
                        st.caption(f"{collection}")

                        img_b64 = render_series_middle_slice(series_uuid)
                        if img_b64 and not str(img_b64).startswith("ERROR"):
                            img_bytes = base64.b64decode(img_b64)
                            st.image(img_bytes, use_container_width=True)
                        else:
                            st.info("Unable to render preview")

            # Clinical context panel
            with st.container(border=True):
                st.markdown("##### :material/ecg_heart: Clinical Context")
                st.caption("Procedure outcomes for patients linked to matched imaging series")

                series_uuids = [r.get("SERIES_UUID", "") for r in unique_results]
                # Map series UUIDs to patients via DICOM metadata
                uuid_list = "', '".join(series_uuids)
                clinical_df = run_query(f"""
                    SELECT DISTINCT
                        m.SERIES_LABEL, m.PATIENT_ID,
                        p.PROCEDURE_DATE, p.OUTCOME, p.ACCESS_ROUTE, p.LOS_DAYS,
                        d.MODEL_NAME, d.VALVE_SIZE_MM,
                        ae.EVENT_TYPE, ae.SEVERITY
                    FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA m
                    JOIN EW_IMAGING_DB.EXPLORER.FACT_PROCEDURE p ON m.PATIENT_ID = p.PATIENT_ID
                    JOIN EW_IMAGING_DB.EXPLORER.DIM_DEVICE d ON p.DEVICE_ID = d.DEVICE_ID
                    LEFT JOIN EW_IMAGING_DB.EXPLORER.FACT_ADVERSE_EVENT ae ON p.PATIENT_ID = ae.PATIENT_ID
                    WHERE m.SERIES_LABEL IN (
                        SELECT DISTINCT SERIES_LABEL
                        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
                        WHERE SPLIT_PART(FILE_NAME, '/', 1) IN ('{uuid_list}')
                    )
                    ORDER BY p.PROCEDURE_DATE
                """)
                if not clinical_df.empty:
                    st.dataframe(clinical_df, hide_index=True, use_container_width=True)
                else:
                    st.info("No linked TAVR procedures found for these series.")
        else:
            st.warning("No results found. Try a different query.")


# =========================================================================
# TAB 6: GOVERNANCE DASHBOARD
# =========================================================================
with tab6:
    sf_section_header("Governance Dashboard", "PHI protection across imaging AND clinical data — one policy, one platform")

    with st.container(border=True):
        st.markdown("##### :material/lock: PHI Masking — Consistent Across All Tables")
        st.caption("Same PHI_MASK policy enforced on imaging metadata AND clinical patient records")

        g1, g2 = st.columns(2)
        with g1:
            st.markdown(f"**SYSADMIN View** — Full Access")
            phi_imaging = run_query("""
                SELECT DISTINCT 'DICOM_CARDIAC_METADATA' AS SOURCE_TABLE, PATIENT_ID, PATIENT_NAME
                FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
                ORDER BY PATIENT_ID
            """)
            phi_clinical = run_query("""
                SELECT 'DIM_PATIENT' AS SOURCE_TABLE, PATIENT_ID, FULL_NAME AS PATIENT_NAME
                FROM EW_IMAGING_DB.EXPLORER.DIM_PATIENT
                ORDER BY PATIENT_ID
            """)
            phi_combined = pd.concat([phi_imaging, phi_clinical], ignore_index=True)
            st.dataframe(phi_combined, hide_index=True, use_container_width=True)

        with g2:
            st.markdown(f"**ANALYST View** — Masked")
            masked_combined = phi_combined.copy()
            masked_combined["PATIENT_ID"] = "***MASKED***"
            masked_combined["PATIENT_NAME"] = "***MASKED***"
            st.dataframe(masked_combined, hide_index=True, use_container_width=True)

    with st.container(border=True):
        st.markdown("##### :material/fingerprint: Data Classification")
        st.caption("SYSTEM$CLASSIFY identifies PHI columns across imaging and clinical tables")
        classification_data = pd.DataFrame({
            "TABLE": ["DICOM_CARDIAC_METADATA", "DICOM_CARDIAC_METADATA", "DICOM_CARDIAC_METADATA", "DICOM_CARDIAC_METADATA", "DICOM_CARDIAC_METADATA",
                       "DIM_PATIENT", "DIM_PATIENT", "DIM_PATIENT", "DIM_PHYSICIAN"],
            "COLUMN": ["PATIENT_ID", "PATIENT_NAME", "PATIENT_SEX", "PATIENT_AGE", "STUDY_DATE",
                        "FULL_NAME", "DATE_OF_BIRTH", "SEX", "PHYSICIAN_NAME"],
            "PRIVACY_CATEGORY": ["IDENTIFIER", "IDENTIFIER", "QUASI_IDENTIFIER", "QUASI_IDENTIFIER", "QUASI_IDENTIFIER",
                                  "IDENTIFIER", "QUASI_IDENTIFIER", "QUASI_IDENTIFIER", "IDENTIFIER"],
            "MASKING_APPLIED": ["YES", "YES", "NO", "NO", "NO",
                                 "YES", "NO", "NO", "YES"],
        })
        st.dataframe(classification_data, hide_index=True, use_container_width=True)

    with st.container(border=True):
        st.markdown("##### :material/conversion_path: Data Lineage")
        st.caption("End-to-end lineage from raw DICOM files to analytics")
        st.code("""
    IDC Open Data (S3)
        │
        ▼
    External Stage (IDC_OPEN_DATA_ALL_STG)
        │
        ▼
    Directory Table (REFRESH SUBPATH per series)
        │
        ▼
    Python UDF: EXTRACT_DICOM_METADATA (pydicom)
        │
        ▼
    DICOM_CARDIAC_METADATA (structured table)
        │
        ├── Masking Policy (PATIENT_ID, PATIENT_NAME)
        ├── Row Access Policy (by study)
        │
        ├──▶ AI_CLASSIFY → Study Type Classification
        ├──▶ AI_COMPLETE → Clinical Summaries
        │
        └──▶ Streamlit Dashboard + Cortex Agent
        """, language="text")


# =========================================================================
# FLOATING CORTEX AGENT CHAT
# =========================================================================
if "agent_messages" not in st.session_state:
    st.session_state.agent_messages = [
        {"role": "ai", "content": "Hi! I'm your DICOM imaging analyst. Ask me anything about the dataset."},
    ]
if "agent_pending_query" not in st.session_state:
    st.session_state.agent_pending_query = None


def _handle_agent_submit():
    user_input = st.session_state.get("agent_chat_input", "").strip()
    if not user_input:
        return
    st.session_state.agent_messages.append({"role": "user", "content": user_input})
    st.session_state.agent_pending_query = user_input


def _submit_example_prompt(prompt):
    st.session_state.agent_messages.append({"role": "user", "content": prompt})
    st.session_state.agent_pending_query = prompt


AGENT_DB = "EW_IMAGING_DB"
AGENT_SCHEMA = "EXPLORER"
AGENT_NAME = "DICOM_IMAGING_AGENT"
AGENT_PATH = f"/api/v2/databases/{AGENT_DB}/schemas/{AGENT_SCHEMA}/agents/{AGENT_NAME}:run"


def _build_agent_payload(user_input):
    return {
        "stream": True,
        "messages": [{"role": "user", "content": [{"type": "text", "text": user_input}]}],
    }


def _iter_sse_text(resp):
    current_event = ""
    yielded_any = False
    final_text = ""
    for raw_line in resp.iter_lines(decode_unicode=True):
        if not raw_line:
            current_event = ""
            continue
        if raw_line.startswith("event: "):
            current_event = raw_line[7:]
            continue
        if raw_line.startswith("data: "):
            data_str = raw_line[6:]
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue
            print(f"[SSE] event={current_event} keys={list(data.keys())[:5]}")
            if current_event == "response.text.delta" and "text" in data:
                yielded_any = True
                yield data["text"]
            elif current_event == "response":
                content = data.get("content", [])
                for item in content:
                    if item.get("type") == "text" and item.get("text"):
                        final_text = item["text"]
    if not yielded_any and final_text:
        yield final_text
    if not yielded_any and not final_text:
        print("[SSE] WARNING: no text yielded from stream")


def _stream_agent_response(user_input):
    payload = _build_agent_payload(user_input)
    if IS_SIS:
        token = open("/snowflake/session/token", "r").read()
        host = os.getenv("SNOWFLAKE_HOST")
        url = f"https://{host}{AGENT_PATH}"
        api_headers = {
            "Authorization": f"Bearer {token}",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        resp = requests.post(url, headers=api_headers, json=payload, timeout=120, stream=True)
        if resp.status_code != 200:
            print(f"[FALLBACK] Agent API returned {resp.status_code}, falling back to AI_COMPLETE")
            raise RuntimeError(f"Agent API returned {resp.status_code}")
        print(f"[AGENT-SIS] Streaming SSE response for: {user_input[:80]}")
        yield from _iter_sse_text(resp)
    else:
        conn = get_connection()
        rest = conn._rest
        token = rest._token
        url = f"{rest._protocol}://{rest._host}:{rest._port}{AGENT_PATH}"
        api_headers = {
            "Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        with rest.use_requests_session() as session:
            resp = session.post(url, headers=api_headers, json=payload, timeout=120, stream=True)
            if resp.status_code != 200:
                print(f"[FALLBACK] Agent API returned {resp.status_code}, falling back to AI_COMPLETE")
                raise RuntimeError(f"Agent API returned {resp.status_code}")
            print(f"[AGENT] Streaming SSE response for: {user_input[:80]}")
            yield from _iter_sse_text(resp)


def _run_agent_fallback(user_input):
    safe_input = user_input.replace("'", "''")
    result = run_query(f"""
        SELECT AI_COMPLETE('claude-4-sonnet',
            'You are a cardiac imaging data analyst. Dataset: 2,848 DICOM files, 10 CT series, 10 patients, 5 collections, 4 manufacturers (SIEMENS, GE, TOSHIBA, Philips). Question: {safe_input}') AS RESPONSE
    """)
    if not result.empty:
        return _clean_response(result.iloc[0]["RESPONSE"])
    return "Unable to generate response."


agent_panel = FloatingContainer(
    icon=":material/smart_toy:",
    label="Cortex Agent",
    start_position="bottom",
    key="cortex_agent_chat",
    glassmorphic=False,
)

with agent_panel.panel():
    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    pending = st.session_state.agent_pending_query
    if pending:
        st.session_state.agent_pending_query = None
        with st.chat_message("ai"):
            try:
                status_placeholder = st.empty()
                status_placeholder.caption(":material/hourglass_top: Thinking...")
                with st.expander(":material/terminal: Agent trace", expanded=False):
                    full_response = st.write_stream(_stream_agent_response(pending))
                full_response = _clean_response(full_response) if full_response else "No response received."
                status_placeholder.empty()
            except Exception as e:
                print(f"[FALLBACK] SSE stream error: {e}, falling back to AI_COMPLETE")
                status_placeholder.empty()
                full_response = _run_agent_fallback(pending)
        st.session_state.agent_messages.append({"role": "ai", "content": full_response})
        st.rerun()

    has_user_message = any(m["role"] == "user" for m in st.session_state.agent_messages)
    if not has_user_message:
        example_prompts = [
            "Find scans showing aortic valve calcification",
            "Which manufacturers are represented?",
            "Show me contrast-enhanced cardiac CTA images",
            "What is the average slice thickness?",
        ]
        pcols = st.columns(2)
        for i, p in enumerate(example_prompts):
            pcols[i % 2].button(p, key=f"example_{i}", use_container_width=True, on_click=_submit_example_prompt, args=(p,))

    st.chat_input(
        "Ask about cardiac imaging data...",
        key="agent_chat_input",
        on_submit=_handle_agent_submit,
    )
