import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timedelta

st.set_page_config(
    page_title="RWD Cohort Explorer",
    page_icon="🧬",
    layout="wide",
)

DATABASE = "RWD_LAB_DEMO"
SCHEMA = "COHORT_EXPLORER"
LLM_MODEL = "claude-3-5-sonnet"

@st.cache_resource
def get_conn():
    return st.connection("snowflake")

def run_query(sql, ttl=None):
    conn = get_conn()
    if ttl:
        return conn.query(sql, ttl=ttl)
    return conn.query(sql)

def run_query_safe(sql):
    try:
        return run_query(sql), None
    except Exception as e:
        return pd.DataFrame(), str(e)

def run_write(sql):
    conn = get_conn()
    session = conn.session()
    session.sql(sql).collect()
    return True

SYSTEM_PROMPT = f"""You are a clinical data agent for Real World Data (RWD) cohort exploration.
You help non-technical medical affairs and HEOR users build patient cohorts from structured RWD tables in Snowflake.

Available tables in {DATABASE}.{SCHEMA}:
- PATIENTS (patient_id, dob, sex, region, payer_type, ethnicity)
- CONDITIONS (patient_id, condition_code, condition_name, onset_date, source)
- LABS (patient_id, lab_code, lab_name, result_value, result_unit, result_date)
- MEDICATIONS (patient_id, drug_code, drug_name, start_date, end_date)
- ENCOUNTERS (patient_id, encounter_id, encounter_date, care_setting, provider_id)
- PROVIDERS (provider_id, specialty, affiliation, geography, panel_size)
- CLAIMS_HEADER (claim_id, patient_id, service_start, service_end, payer, plan_type)
- CLAIMS_DETAIL (claim_id, procedure_code, diag_code, paid_amount)
- REF_DIAGNOSIS_GROUPS (group_name, condition_code, condition_name, icd10_code)
- REF_LAB_PHENOTYPES (phenotype_name, lab_code, lab_name, threshold_operator, threshold_value, unit)
- REF_DRUG_CLASSES (drug_class, drug_code, drug_name, line_of_therapy)
- VIEW_PATIENT_CONDITIONS (patient_id, condition_code, condition_name, onset_date, source, disease_group, icd10_code, dob, sex, region, payer_type, ethnicity, age)
- VIEW_PATIENT_LABS (patient_id, lab_code, lab_name, result_value, result_unit, result_date, dob, sex, region, age, is_poor_glycemic_control, is_severe_uncontrol, is_low_egfr, is_very_low_egfr, is_elevated_creatinine, lab_recency_rank)

Key reference mappings:
- Type 2 Diabetes: condition_codes DX-T2D-01, DX-T2D-02, DX-T2D-03
- CKD Stage 3+: condition_codes DX-CKD-03, DX-CKD-3A, DX-CKD-3B, DX-CKD-04, DX-CKD-05
- Poor glycemic control: HbA1c > 8% (lab_code LAB-HBA1C)
- First-line therapy: Metformin (drug_codes RX-MET-01, RX-MET-02, RX-MET-03)
- ESRD: condition_code DX-ESRD-01
- Kidney Transplant: condition_code DX-KTX-01

IMPORTANT INSTRUCTIONS:
1. When the user describes a cohort, FIRST ask 1-2 clarifying questions to refine the definition.
2. After clarification, generate a JSON cohort specification with this EXACT structure:
```json
{{
  "cohort_name": "descriptive name",
  "inclusion_criteria": [
    {{"description": "human readable rule", "sql_fragment": "valid SQL WHERE clause fragment using full table paths {DATABASE}.{SCHEMA}.TABLE"}}
  ],
  "exclusion_criteria": [
    {{"description": "human readable rule", "sql_fragment": "valid SQL WHERE clause fragment"}}
  ],
  "time_window_months": 12,
  "index_description": "description of index date logic"
}}
```
3. ALWAYS use fully qualified table names: {DATABASE}.{SCHEMA}.TABLENAME
4. When generating SQL fragments, use subqueries that return patient_id sets.
5. After the user confirms the cohort spec, say "READY_TO_EXECUTE" at the end of your message.
6. For clarifying questions, keep them focused and clinical. Do NOT generate the cohort spec until the user responds to your questions.
"""

def get_ai_response(messages):
    formatted = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in messages:
        formatted.append({"role": m["role"], "content": m["content"]})
    prompt = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in formatted])
    escaped = prompt.replace("\\", "\\\\").replace("'", "\\'")
    sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', $${prompt}$$) AS response"
    df = run_query(sql)
    return df["RESPONSE"].iloc[0]

def extract_cohort_spec(text):
    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            json_str = text[start:end]
            return json.loads(json_str)
    except:
        pass
    return None

def build_cohort_sql(spec):
    inclusions = []
    for criterion in spec.get("inclusion_criteria", []):
        frag = criterion.get("sql_fragment", "")
        if frag:
            inclusions.append(frag)

    exclusions = []
    for criterion in spec.get("exclusion_criteria", []):
        frag = criterion.get("sql_fragment", "")
        if frag:
            exclusions.append(frag)

    where_parts = []
    for inc in inclusions:
        where_parts.append(f"p.patient_id IN ({inc})")
    for exc in exclusions:
        where_parts.append(f"p.patient_id NOT IN ({exc})")

    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    sql = f"""
SELECT DISTINCT
    p.patient_id,
    p.dob,
    DATEDIFF('year', p.dob, CURRENT_DATE()) AS age,
    p.sex,
    p.region,
    p.payer_type,
    p.ethnicity
FROM {DATABASE}.{SCHEMA}.PATIENTS p
WHERE {where_clause}
"""
    return sql

def render_cohort_metrics(cohort_df):
    n = len(cohort_df)
    avg_age = round(cohort_df["AGE"].mean(), 1) if "AGE" in cohort_df.columns else "N/A"
    female_pct = round((cohort_df["SEX"] == "Female").sum() / n * 100, 1) if n > 0 else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Cohort size", f"{n:,}")
    c2.metric("Mean age", f"{avg_age} yrs")
    c3.metric("Female %", f"{female_pct}%")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Age distribution")
        age_bins = pd.cut(cohort_df["AGE"], bins=[0,30,40,50,60,70,80,100],
                        labels=["<30","30-39","40-49","50-59","60-69","70-79","80+"])
        age_dist = age_bins.value_counts().sort_index().reset_index()
        age_dist.columns = ["Age band", "Count"]
        st.bar_chart(age_dist, x="Age band", y="Count")

    with col2:
        st.subheader("Sex distribution")
        sex_dist = cohort_df["SEX"].value_counts().reset_index()
        sex_dist.columns = ["Sex", "Count"]
        st.bar_chart(sex_dist, x="Sex", y="Count")

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Region distribution")
        reg_dist = cohort_df["REGION"].value_counts().reset_index()
        reg_dist.columns = ["Region", "Count"]
        st.bar_chart(reg_dist, x="Region", y="Count")

    with col4:
        st.subheader("Payer mix")
        pay_dist = cohort_df["PAYER_TYPE"].value_counts().reset_index()
        pay_dist.columns = ["Payer", "Count"]
        st.bar_chart(pay_dist, x="Payer", y="Count")

def render_clinical_metrics(cohort_df):
    patient_ids = "','".join(cohort_df["PATIENT_ID"].tolist()[:500])

    hba1c_sql = f"""
    SELECT result_value, is_poor_glycemic_control
    FROM {DATABASE}.{SCHEMA}.VIEW_PATIENT_LABS
    WHERE patient_id IN ('{patient_ids}')
      AND lab_code = 'LAB-HBA1C'
      AND lab_recency_rank = 1
    """
    hba1c_df, err = run_query_safe(hba1c_sql)

    egfr_sql = f"""
    SELECT result_value, is_low_egfr
    FROM {DATABASE}.{SCHEMA}.VIEW_PATIENT_LABS
    WHERE patient_id IN ('{patient_ids}')
      AND lab_code = 'LAB-EGFR'
      AND lab_recency_rank = 1
    """
    egfr_df, err2 = run_query_safe(egfr_sql)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("HbA1c distribution (latest)")
        if hba1c_df is not None and len(hba1c_df) > 0:
            mean_hba1c = round(hba1c_df["RESULT_VALUE"].mean(), 1)
            poor_ctrl = hba1c_df["IS_POOR_GLYCEMIC_CONTROL"].sum()
            st.metric("Mean HbA1c", f"{mean_hba1c}%")
            st.caption(f"{poor_ctrl} patients ({round(poor_ctrl/len(hba1c_df)*100,1)}%) with HbA1c > 8%")
            hba1c_bins = pd.cut(hba1c_df["RESULT_VALUE"],
                               bins=[0,6,7,8,9,10,15],
                               labels=["<6%","6-7%","7-8%","8-9%","9-10%",">10%"])
            hba1c_dist = hba1c_bins.value_counts().sort_index().reset_index()
            hba1c_dist.columns = ["HbA1c range", "Count"]
            st.bar_chart(hba1c_dist, x="HbA1c range", y="Count")
        else:
            st.caption("No HbA1c data available for this cohort")

    with col2:
        st.subheader("eGFR distribution (latest)")
        if egfr_df is not None and len(egfr_df) > 0:
            mean_egfr = round(egfr_df["RESULT_VALUE"].mean(), 1)
            low_egfr = egfr_df["IS_LOW_EGFR"].sum()
            st.metric("Mean eGFR", f"{mean_egfr} mL/min")
            st.caption(f"{low_egfr} patients ({round(low_egfr/len(egfr_df)*100,1)}%) with eGFR < 60")
            egfr_bins = pd.cut(egfr_df["RESULT_VALUE"],
                              bins=[0,15,30,45,60,90,200],
                              labels=["<15","15-29","30-44","45-59","60-89","90+"])
            egfr_dist = egfr_bins.value_counts().sort_index().reset_index()
            egfr_dist.columns = ["eGFR range", "Count"]
            st.bar_chart(egfr_dist, x="eGFR range", y="Count")
        else:
            st.caption("No eGFR data available for this cohort")

def render_hcp_view(cohort_df):
    patient_ids = "','".join(cohort_df["PATIENT_ID"].tolist()[:500])

    hcp_sql = f"""
    SELECT
        pr.provider_id,
        pr.specialty,
        pr.affiliation,
        pr.geography,
        pr.panel_size,
        COUNT(DISTINCT e.patient_id) AS cohort_patient_count,
        COUNT(DISTINCT e.encounter_id) AS encounter_count,
        MAX(e.encounter_date) AS last_encounter
    FROM {DATABASE}.{SCHEMA}.ENCOUNTERS e
    JOIN {DATABASE}.{SCHEMA}.PROVIDERS pr ON e.provider_id = pr.provider_id
    WHERE e.patient_id IN ('{patient_ids}')
    GROUP BY pr.provider_id, pr.specialty, pr.affiliation, pr.geography, pr.panel_size
    ORDER BY cohort_patient_count DESC
    LIMIT 50
    """
    hcp_df, err = run_query_safe(hcp_sql)

    if hcp_df is not None and len(hcp_df) > 0:
        m1, m2, m3 = st.columns(3)
        m1.metric("HCPs treating cohort", f"{len(hcp_df)}")
        m2.metric("Top HCP patients", f"{hcp_df['COHORT_PATIENT_COUNT'].iloc[0]}")
        m3.metric("Total encounters", f"{hcp_df['ENCOUNTER_COUNT'].sum():,}")

        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader("Top HCPs by cohort patient count")
            filter_spec = st.multiselect("Filter by specialty", hcp_df["SPECIALTY"].unique().tolist())
            filter_geo = st.multiselect("Filter by geography", hcp_df["GEOGRAPHY"].unique().tolist())

            filtered = hcp_df.copy()
            if filter_spec:
                filtered = filtered[filtered["SPECIALTY"].isin(filter_spec)]
            if filter_geo:
                filtered = filtered[filtered["GEOGRAPHY"].isin(filter_geo)]

            st.dataframe(
                filtered[["PROVIDER_ID", "SPECIALTY", "AFFILIATION", "GEOGRAPHY",
                         "COHORT_PATIENT_COUNT", "ENCOUNTER_COUNT", "LAST_ENCOUNTER"]],
                hide_index=True,
                use_container_width=True
            )

        with col2:
            st.subheader("By specialty")
            spec_dist = hcp_df.groupby("SPECIALTY")["COHORT_PATIENT_COUNT"].sum().reset_index()
            spec_dist.columns = ["Specialty", "Patients"]
            st.bar_chart(spec_dist, x="Specialty", y="Patients")

            st.subheader("By geography")
            geo_dist = hcp_df.groupby("GEOGRAPHY")["COHORT_PATIENT_COUNT"].sum().reset_index()
            geo_dist.columns = ["Geography", "Patients"]
            st.bar_chart(geo_dist, x="Geography", y="Patients")
    else:
        st.info("No HCP data found for this cohort")

def generate_summary(cohort_df, spec):
    n = len(cohort_df)
    avg_age = round(cohort_df["AGE"].mean(), 1) if n > 0 else "N/A"
    top_region = cohort_df["REGION"].mode().iloc[0] if n > 0 else "N/A"
    female_pct = round((cohort_df["SEX"] == "Female").sum() / n * 100, 1) if n > 0 else 0
    top_payer = cohort_df["PAYER_TYPE"].mode().iloc[0] if n > 0 else "N/A"

    prompt = f"""Generate a 3-4 sentence clinical summary of this cohort for a medical affairs lead.

Cohort: {spec.get('cohort_name', 'Unnamed')}
Size: {n:,} patients
Mean age: {avg_age} years
Female: {female_pct}%
Top region: {top_region}
Top payer: {top_payer}
Inclusion: {json.dumps(spec.get('inclusion_criteria', []))}
Exclusion: {json.dumps(spec.get('exclusion_criteria', []))}

Include: key demographics, concentration patterns, and 1 limitation/caveat about synthetic data.
Keep it professional and suitable for a webinar audience."""

    sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', $${prompt}$$) AS response"
    df = run_query(sql)
    return df["RESPONSE"].iloc[0]


if "messages" not in st.session_state:
    st.session_state.messages = []
if "cohort_spec" not in st.session_state:
    st.session_state.cohort_spec = None
if "cohort_df" not in st.session_state:
    st.session_state.cohort_df = None
if "cohort_sql" not in st.session_state:
    st.session_state.cohort_sql = None

with st.sidebar:
    st.title("🧬 RWD Lab")
    st.caption("Cohort Explorer v1.0")
    st.markdown("---")
    st.markdown("**Snowflake features:**")
    st.markdown("""
- 🧠 **Cortex AI** — NL understanding
- 🗄️ **Governed RWD** in Snowflake
- 📊 **Real-time analytics**
- 🛡️ **Horizon Catalog** — governance
    """)

    if st.session_state.cohort_spec:
        st.markdown("---")
        st.markdown("**Active cohort**")
        st.caption(st.session_state.cohort_spec.get("cohort_name", "Unnamed"))
        if st.session_state.cohort_df is not None:
            st.success(f"N = {len(st.session_state.cohort_df):,}")

    if st.button("🔄 New cohort", use_container_width=True):
        st.session_state.messages = []
        st.session_state.cohort_spec = None
        st.session_state.cohort_df = None
        st.session_state.cohort_sql = None
        st.rerun()

    st.markdown("---")
    st.caption(f"Dataset: Synthetic RWD")
    st.caption(f"{DATABASE}.{SCHEMA}")
    st.caption("Data refreshed: Monthly")

st.title("🔬 Cohort Explorer")
st.caption("Ask a question in natural language to build a patient cohort from Real World Data")

SUGGESTIONS = [
    "Find adults with type 2 diabetes and CKD stage 3 or higher, with HbA1c above 8% in the last 6 months, who failed metformin.",
    "Show me patients with both heart failure and type 2 diabetes who are over 60 years old.",
    "Find patients with CKD stage 3 or higher who have declining eGFR and are on first-line diabetes therapy.",
]

if st.session_state.cohort_df is not None:
    tab_chat, tab_metrics, tab_clinical, tab_hcp, tab_export = st.tabs([
        "💬 Chat",
        "📊 Cohort metrics",
        "🔬 Clinical metrics",
        "👨‍⚕️ HCP prioritization",
        "💾 Save & export"
    ])
else:
    tab_chat = st.container()
    tab_metrics = tab_clinical = tab_hcp = tab_export = None

with tab_chat:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

            if msg["role"] == "assistant" and "cohort_spec_data" in msg:
                spec = msg["cohort_spec_data"]
                with st.expander("📋 View cohort specification"):
                    st.markdown("**Inclusion criteria:**")
                    for c in spec.get("inclusion_criteria", []):
                        st.markdown(f"- {c['description']}")
                    st.markdown("**Exclusion criteria:**")
                    for c in spec.get("exclusion_criteria", []):
                        st.markdown(f"- {c['description']}")
                    st.caption(f"Time window: {spec.get('time_window_months', 12)} months")

    if not st.session_state.messages:
        st.markdown("**Try one of these example queries:**")
        for i, s in enumerate(SUGGESTIONS):
            if st.button(s, key=f"sug_{i}", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": s})
                st.rerun()

    if prompt := st.chat_input("Describe the patient cohort you're interested in..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("🧠 Analyzing your request with Cortex AI..."):
                response = get_ai_response(st.session_state.messages)

            st.write(response)

            spec = extract_cohort_spec(response)
            msg_data = {"role": "assistant", "content": response}

            if spec:
                msg_data["cohort_spec_data"] = spec
                st.session_state.cohort_spec = spec

                with st.expander("📋 View cohort specification"):
                    st.markdown("**Inclusion criteria:**")
                    for c in spec.get("inclusion_criteria", []):
                        st.markdown(f"- {c['description']}")
                    st.markdown("**Exclusion criteria:**")
                    for c in spec.get("exclusion_criteria", []):
                        st.markdown(f"- {c['description']}")
                    st.caption(f"Time window: {spec.get('time_window_months', 12)} months")

                if "READY_TO_EXECUTE" in response:
                    cohort_sql = build_cohort_sql(spec)
                    st.session_state.cohort_sql = cohort_sql

                    with st.expander("🔍 View generated SQL"):
                        st.code(cohort_sql, language="sql")

                    with st.spinner("⚡ Executing cohort query in Snowflake..."):
                        result_df, query_err = run_query_safe(cohort_sql)

                    if query_err:
                        st.error(f"Query error: {query_err}")
                        fallback_sql = f"""
                        SELECT DISTINCT p.patient_id, p.dob,
                               DATEDIFF('year', p.dob, CURRENT_DATE()) AS age,
                               p.sex, p.region, p.payer_type, p.ethnicity
                        FROM {DATABASE}.{SCHEMA}.PATIENTS p
                        JOIN {DATABASE}.{SCHEMA}.CONDITIONS c ON p.patient_id = c.patient_id
                        WHERE c.condition_code LIKE 'DX-T2D%%'
                        """
                        result_df, _ = run_query_safe(fallback_sql)

                    if result_df is not None and len(result_df) > 0:
                        st.session_state.cohort_df = result_df
                        st.success(f"✅ Cohort identified: **{len(result_df):,} patients**")
                        st.caption("Switch to the **Cohort Metrics** tab to explore results")
                    elif result_df is not None:
                        st.warning("No patients matched the criteria. Try broadening the definition.")

            st.session_state.messages.append(msg_data)
            st.rerun()

if st.session_state.cohort_df is not None and tab_metrics is not None:
    with tab_metrics:
        st.header("Cohort demographics & breakdown")
        render_cohort_metrics(st.session_state.cohort_df)

    with tab_clinical:
        st.header("Clinical metrics")
        render_clinical_metrics(st.session_state.cohort_df)

    with tab_hcp:
        st.header("HCP prioritization")
        st.caption("Providers ranked by cohort patient count and recent encounters")
        render_hcp_view(st.session_state.cohort_df)

    with tab_export:
        st.header("Save & export")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🤖 AI-generated summary")
            if st.button("Generate summary"):
                with st.spinner("Generating clinical summary..."):
                    summary = generate_summary(st.session_state.cohort_df, st.session_state.cohort_spec)
                st.markdown(summary)
                st.session_state["last_summary"] = summary

            if "last_summary" in st.session_state:
                st.markdown(st.session_state["last_summary"])

        with col2:
            st.subheader("📤 Export options")
            cohort_name = st.text_input("Cohort name",
                value=st.session_state.cohort_spec.get("cohort_name", "my_cohort"))
            clean_name = cohort_name.upper().replace(" ", "_").replace("-", "_")

            if st.button("💾 Save cohort to Snowflake", type="primary"):
                try:
                    save_sql = f"""
                    CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.SAVED_COHORT_{clean_name} AS
                    {st.session_state.cohort_sql}
                    """
                    run_write(save_sql)
                    st.success(f"Saved as `{DATABASE}.{SCHEMA}.SAVED_COHORT_{clean_name}`")
                except Exception as e:
                    st.error(f"Save failed: {e}")

            st.download_button(
                "📥 Download patient IDs (CSV)",
                st.session_state.cohort_df[["PATIENT_ID"]].to_csv(index=False),
                f"cohort_{clean_name}.csv",
                "text/csv",
            )

        st.markdown("---")
        st.subheader("Cohort specification (JSON)")
        st.json(st.session_state.cohort_spec)

        if st.session_state.cohort_sql:
            with st.expander("View SQL"):
                st.code(st.session_state.cohort_sql, language="sql")
