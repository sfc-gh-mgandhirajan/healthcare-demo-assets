import streamlit as st
import pandas as pd
import json
import re
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="RWD Cohort Explorer",
    page_icon="🧬",
    layout="wide",
)

DATABASE = "RWD_LAB_DEMO"
SCHEMA = "COHORT_EXPLORER"
LLM_MODEL = "llama3.1-70b"

session = get_active_session()

def run_query(sql):
    return session.sql(sql).to_pandas()

def run_query_safe(sql):
    try:
        return run_query(sql), None
    except Exception as e:
        return pd.DataFrame(), str(e)

def run_write(sql):
    session.sql(sql).collect()
    return True

SYSTEM_PROMPT = f"""You are a clinical RWD cohort builder. Help users define patient cohorts from Snowflake tables.

Tables in {DATABASE}.{SCHEMA}: PATIENTS(patient_id,dob,sex,region,payer_type,ethnicity), CONDITIONS(patient_id,condition_code,condition_name,onset_date), LABS(patient_id,lab_code,lab_name,result_value,result_unit,result_date), MEDICATIONS(patient_id,drug_code,drug_name,start_date,end_date), ENCOUNTERS(patient_id,encounter_id,encounter_date,care_setting,provider_id), PROVIDERS(provider_id,specialty,affiliation,geography,panel_size).
Views: VIEW_PATIENT_LABS(patient_id,lab_code,result_value,lab_recency_rank,is_poor_glycemic_control,is_low_egfr).

Codes: T2D=DX-T2D-01/02/03, CKD3+=DX-CKD-03/3A/3B/04/05, HbA1c=LAB-HBA1C, eGFR=LAB-EGFR, Metformin=RX-MET-01/02/03, ESRD=DX-ESRD-01.

Rules:
1. When the user first describes a cohort, ask 1-2 brief clarifying questions. Do NOT output JSON yet.
2. After the user answers your questions, generate ONLY the JSON cohort spec below with no extra text:
{{"cohort_name":"name","inclusion_criteria":[{{"description":"rule","sql_fragment":"SELECT patient_id FROM {DATABASE}.{SCHEMA}.TABLE WHERE ..."}}],"exclusion_criteria":[...],"time_window_months":12,"index_description":"..."}}
3. Use fully qualified table names: {DATABASE}.{SCHEMA}.TABLE
4. SQL fragments must be SELECT patient_id subqueries.
5. Be concise. No markdown formatting around the JSON.
"""

def call_cortex(messages_list, max_tokens=1024):
    safe_messages = []
    for m in messages_list:
        safe_msg = dict(m)
        if "content" in safe_msg:
            safe_msg["content"] = safe_msg["content"].replace("$$", "$ $")
        safe_messages.append(safe_msg)
    messages_json = json.dumps(safe_messages, ensure_ascii=False)
    options_json = json.dumps({"max_tokens": max_tokens, "temperature": 0.3})
    sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', PARSE_JSON($${messages_json}$$), PARSE_JSON($${options_json}$$))::VARCHAR AS response"
    df = session.sql(sql).to_pandas()
    raw = df["RESPONSE"].iloc[0]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed.get("choices", [{}])[0].get("messages", raw)
        except:
            return raw
    return str(raw)

def get_ai_response(messages):
    conv = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in messages:
        conv.append({"role": m["role"], "content": m["content"]})
    return call_cortex(conv, max_tokens=1024)

def extract_cohort_spec(text):
    try:
        candidates = re.findall(r'\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}', text, re.DOTALL)
        for candidate in candidates:
            try:
                obj = json.loads(candidate)
                if "cohort_name" in obj and "inclusion_criteria" in obj:
                    return obj
            except:
                continue
    except:
        pass
    try:
        start = text.find('{"cohort_name"')
        if start < 0:
            start = text.find('"cohort_name"')
            if start > 0:
                start = text.rfind("{", 0, start)
        if start >= 0:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == '{':
                    depth += 1
                elif text[i] == '}':
                    depth -= 1
                    if depth == 0:
                        return json.loads(text[start:i+1])
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

    return f"""
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

def execute_cohort(spec):
    cohort_sql = build_cohort_sql(spec)
    st.session_state.cohort_sql = cohort_sql
    result_df, query_err = run_query_safe(cohort_sql)
    if query_err:
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
        st.session_state.phase = "results"
        return True
    return False

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

def render_rwe_insights(cohort_df, spec):
    patient_ids = "','".join(cohort_df["PATIENT_ID"].tolist()[:500])
    n = len(cohort_df)

    st.markdown("**From Real World Data to Real World Evidence** — This tab synthesizes cohort-level signals into actionable insights for medical affairs, HEOR, and field teams.")
    st.markdown("---")

    tx_sql = f"""
    SELECT
        dc.drug_class,
        dc.line_of_therapy,
        COUNT(DISTINCT m.patient_id) AS patient_count,
        ROUND(COUNT(DISTINCT m.patient_id) * 100.0 / {n}, 1) AS pct_of_cohort,
        SUM(CASE WHEN m.end_date IS NOT NULL THEN 1 ELSE 0 END) AS discontinued_count,
        SUM(CASE WHEN m.end_date IS NULL THEN 1 ELSE 0 END) AS active_count
    FROM {DATABASE}.{SCHEMA}.MEDICATIONS m
    JOIN {DATABASE}.{SCHEMA}.REF_DRUG_CLASSES dc ON m.drug_code = dc.drug_code
    WHERE m.patient_id IN ('{patient_ids}')
    GROUP BY dc.drug_class, dc.line_of_therapy
    ORDER BY dc.line_of_therapy, patient_count DESC
    """
    tx_df, tx_err = run_query_safe(tx_sql)

    st.subheader("💊 Treatment landscape")
    if tx_df is not None and len(tx_df) > 0:
        col_chart, col_table = st.columns([1, 1])
        with col_chart:
            st.markdown("**Therapy utilization (% of cohort)**")
            chart_data = tx_df[["DRUG_CLASS", "PCT_OF_COHORT"]].copy()
            chart_data.columns = ["Drug class", "% of cohort"]
            st.bar_chart(chart_data, x="Drug class", y="% of cohort")
        with col_table:
            st.markdown("**By line of therapy**")
            for lot in ["First-line", "Second-line", "Third-line", "Renoprotective", "Lipid-lowering"]:
                lot_df = tx_df[tx_df["LINE_OF_THERAPY"] == lot]
                if len(lot_df) > 0:
                    drugs = ", ".join([f"{r['DRUG_CLASS']} ({r['PCT_OF_COHORT']}%)" for _, r in lot_df.iterrows()])
                    st.markdown(f"**{lot}:** {drugs}")

        disc_rate = tx_df["DISCONTINUED_COUNT"].sum() / max(tx_df[["DISCONTINUED_COUNT","ACTIVE_COUNT"]].sum().sum(), 1) * 100
        st.caption(f"Overall therapy discontinuation rate: {disc_rate:.1f}% of prescriptions")
    else:
        st.caption("No medication data available for this cohort")

    st.markdown("---")
    st.subheader("🚨 Unmet need signals")

    hba1c_sql = f"""
    SELECT
        COUNT(*) AS total,
        AVG(result_value) AS mean_hba1c,
        SUM(CASE WHEN result_value > 8 THEN 1 ELSE 0 END) AS poor_control,
        SUM(CASE WHEN result_value > 9 THEN 1 ELSE 0 END) AS severe_uncontrol,
        SUM(CASE WHEN result_value > 10 THEN 1 ELSE 0 END) AS very_severe
    FROM {DATABASE}.{SCHEMA}.VIEW_PATIENT_LABS
    WHERE patient_id IN ('{patient_ids}')
      AND lab_code = 'LAB-HBA1C'
      AND lab_recency_rank = 1
    """
    hba1c_stats, _ = run_query_safe(hba1c_sql)

    egfr_sql = f"""
    SELECT
        COUNT(*) AS total,
        AVG(result_value) AS mean_egfr,
        SUM(CASE WHEN result_value < 60 THEN 1 ELSE 0 END) AS low_egfr,
        SUM(CASE WHEN result_value < 30 THEN 1 ELSE 0 END) AS very_low_egfr
    FROM {DATABASE}.{SCHEMA}.VIEW_PATIENT_LABS
    WHERE patient_id IN ('{patient_ids}')
      AND lab_code = 'LAB-EGFR'
      AND lab_recency_rank = 1
    """
    egfr_stats, _ = run_query_safe(egfr_sql)

    gap_sql = f"""
    SELECT COUNT(DISTINCT m.patient_id) AS patients_on_1L_only
    FROM {DATABASE}.{SCHEMA}.MEDICATIONS m
    JOIN {DATABASE}.{SCHEMA}.REF_DRUG_CLASSES dc ON m.drug_code = dc.drug_code
    WHERE m.patient_id IN ('{patient_ids}')
      AND dc.line_of_therapy = 'First-line'
      AND m.patient_id NOT IN (
          SELECT m2.patient_id
          FROM {DATABASE}.{SCHEMA}.MEDICATIONS m2
          JOIN {DATABASE}.{SCHEMA}.REF_DRUG_CLASSES dc2 ON m2.drug_code = dc2.drug_code
          WHERE dc2.line_of_therapy = 'Second-line'
      )
    """
    gap_df, _ = run_query_safe(gap_sql)

    signals = []

    if hba1c_stats is not None and len(hba1c_stats) > 0 and hba1c_stats["TOTAL"].iloc[0] > 0:
        total_h = int(hba1c_stats["TOTAL"].iloc[0])
        poor = int(hba1c_stats["POOR_CONTROL"].iloc[0])
        severe = int(hba1c_stats["SEVERE_UNCONTROL"].iloc[0])
        poor_pct = round(poor / total_h * 100, 1)
        severe_pct = round(severe / total_h * 100, 1)
        signals.append({"Signal": "Poor glycemic control (HbA1c > 8%)", "Patients": f"{poor:,}", "Rate": f"{poor_pct}%", "Severity": "High" if poor_pct > 30 else "Moderate"})
        signals.append({"Signal": "Severe uncontrol (HbA1c > 9%)", "Patients": f"{severe:,}", "Rate": f"{severe_pct}%", "Severity": "High" if severe_pct > 15 else "Moderate"})

    if egfr_stats is not None and len(egfr_stats) > 0 and egfr_stats["TOTAL"].iloc[0] > 0:
        total_e = int(egfr_stats["TOTAL"].iloc[0])
        low = int(egfr_stats["LOW_EGFR"].iloc[0])
        vlow = int(egfr_stats["VERY_LOW_EGFR"].iloc[0])
        low_pct = round(low / total_e * 100, 1)
        vlow_pct = round(vlow / total_e * 100, 1)
        signals.append({"Signal": "Renal impairment (eGFR < 60)", "Patients": f"{low:,}", "Rate": f"{low_pct}%", "Severity": "High" if low_pct > 20 else "Moderate"})
        signals.append({"Signal": "Severe renal impairment (eGFR < 30)", "Patients": f"{vlow:,}", "Rate": f"{vlow_pct}%", "Severity": "High" if vlow_pct > 5 else "Low"})

    if gap_df is not None and len(gap_df) > 0:
        on_1l_only = int(gap_df["PATIENTS_ON_1L_ONLY"].iloc[0])
        gap_pct = round(on_1l_only / n * 100, 1)
        signals.append({"Signal": "On first-line only (no intensification)", "Patients": f"{on_1l_only:,}", "Rate": f"{gap_pct}%", "Severity": "High" if gap_pct > 20 else "Moderate"})

    if signals:
        signals_df = pd.DataFrame(signals)
        st.dataframe(signals_df, use_container_width=True)
    else:
        st.caption("Insufficient lab/medication data to compute unmet need signals")

    st.markdown("---")
    st.subheader("📝 AI evidence brief")
    if st.button("Generate RWE evidence brief", type="primary"):
        tx_summary = ""
        if tx_df is not None and len(tx_df) > 0:
            tx_summary = tx_df[["DRUG_CLASS","LINE_OF_THERAPY","PCT_OF_COHORT"]].to_string(index=False)
        signal_summary = ""
        if signals:
            signal_summary = pd.DataFrame(signals).to_string(index=False)

        evidence_prompt = f"""Generate a concise Real World Evidence (RWE) brief for a medical affairs audience.

Cohort: {spec.get('cohort_name', 'Unnamed')}
Size: {n:,} patients
Mean age: {round(cohort_df['AGE'].mean(), 1)} years
Female: {round((cohort_df['SEX'] == 'Female').sum() / n * 100, 1)}%

Treatment landscape:
{tx_summary}

Unmet need signals:
{signal_summary}

Inclusion criteria: {json.dumps([c.get('description','') for c in spec.get('inclusion_criteria',[])])}
Exclusion criteria: {json.dumps([c.get('description','') for c in spec.get('exclusion_criteria',[])])}

Structure the brief as:
1. COHORT OVERVIEW (2 sentences)
2. KEY FINDINGS (3-4 bullet points on treatment patterns and unmet need)
3. EVIDENCE IMPLICATIONS (2 sentences on what this means for medical strategy)
4. LIMITATIONS (1 sentence noting synthetic data)

Keep it professional, data-driven, and suitable for a webinar slide."""

        with st.spinner("Generating RWE evidence brief..."):
            brief = call_cortex([{"role": "user", "content": evidence_prompt}], max_tokens=768)
        st.session_state["rwe_brief"] = brief

    if "rwe_brief" in st.session_state:
        st.markdown(st.session_state["rwe_brief"])


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

    msgs = [{"role": "user", "content": prompt}]
    return call_cortex(msgs, max_tokens=512)


for key, default in [
    ("messages", []),
    ("cohort_spec", None),
    ("cohort_df", None),
    ("cohort_sql", None),
    ("pending_prompt", None),
    ("phase", "describe"),
    ("pending_confirm", False),
    ("pending_refine", False),
]:
    if key not in st.session_state:
        st.session_state[key] = default

def reset_all():
    st.session_state.messages = []
    st.session_state.cohort_spec = None
    st.session_state.cohort_df = None
    st.session_state.cohort_sql = None
    st.session_state.pending_prompt = None
    st.session_state.phase = "describe"
    st.session_state.pending_confirm = False
    st.session_state.pending_refine = False
    if "last_summary" in st.session_state:
        del st.session_state["last_summary"]
    if "rwe_brief" in st.session_state:
        del st.session_state["rwe_brief"]

with st.sidebar:
    st.title("🧬 RWD Lab")
    st.caption("Cohort Explorer v1.0")

    st.markdown("---")
    phase = st.session_state.phase
    steps = [
        ("1. Describe", "describe"),
        ("2. Review", "review"),
        ("3. Explore", "results"),
    ]
    for label, step_key in steps:
        if step_key == phase:
            st.markdown(f"**>>> {label}** (current)")
        elif steps.index((label, step_key)) < [s[1] for s in steps].index(phase):
            st.markdown(f"~~{label}~~")
        else:
            st.markdown(f"{label}")

    st.markdown("---")
    st.markdown("**Snowflake features**")
    st.markdown("""
- 🧠 **Cortex AI** — NL cohort builder
- 🗄️ **Governed RWD** — no data movement
- 📊 **Real-time analytics**
- 🛡️ **Horizon** — governance & access
    """)

    if st.session_state.cohort_spec:
        st.markdown("---")
        st.markdown("**Active cohort**")
        st.caption(st.session_state.cohort_spec.get("cohort_name", "Unnamed"))
        if st.session_state.cohort_df is not None:
            st.success(f"N = {len(st.session_state.cohort_df):,}")

    st.markdown("---")
    if st.button("🔄 Start over", use_container_width=True):
        reset_all()
        st.experimental_rerun()

    st.caption("Dataset: Synthetic RWD")
    st.caption(f"{DATABASE}.{SCHEMA}")

st.title("🔬 Cohort Explorer")

SUGGESTIONS = [
    "Find adults with type 2 diabetes and CKD stage 3 or higher, with HbA1c above 8%, who failed metformin.",
    "Show me patients with both heart failure and type 2 diabetes who are over 60 years old.",
    "Find patients with CKD stage 3+ who have low eGFR and are on first-line diabetes therapy.",
]

if st.session_state.pending_confirm:
    st.session_state.pending_confirm = False
    spec = st.session_state.cohort_spec
    if spec:
        with st.spinner("🔍 Running cohort query on Snowflake..."):
            success = execute_cohort(spec)
        if success:
            st.experimental_rerun()
        else:
            st.error("Cohort query returned no results. Try refining your criteria.")
            st.session_state.phase = "review"

if st.session_state.pending_refine:
    st.session_state.pending_refine = False
    st.session_state.cohort_spec = None
    st.session_state.phase = "describe"
    st.session_state.messages.append({
        "role": "user",
        "content": "I want to refine the cohort criteria. Please ask me what I'd like to change."
    })
    with st.spinner("🧠 Cortex AI is ready to help refine..."):
        response = get_ai_response(st.session_state.messages)
    st.session_state.messages.append({"role": "assistant", "content": response})
    st.experimental_rerun()


if st.session_state.phase == "describe":
    st.caption("Step 1 of 3 — Describe your target patient population in plain language")

    if not st.session_state.messages:
        st.markdown("**Quick start — click an example:**")
        for i, s in enumerate(SUGGESTIONS):
            if st.button(s, key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_prompt = s
                st.experimental_rerun()

    for msg in st.session_state.messages:
        avatar = "🧑" if msg["role"] == "user" else "🤖"
        label = "You" if msg["role"] == "user" else "Cortex AI"
        st.markdown(f"**{avatar} {label}:**")
        st.markdown(msg["content"])
        st.markdown("---")

    col_input, col_btn = st.columns([5, 1])
    with col_input:
        user_input = st.text_input(
            "Describe your cohort...",
            key="user_input",
            label_visibility="collapsed",
            placeholder="Describe the patient cohort you're interested in..."
        )
    with col_btn:
        send_clicked = st.button("🚀 Send", type="primary", use_container_width=True)

    prompt_to_process = None
    if st.session_state.pending_prompt:
        prompt_to_process = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
    elif send_clicked and user_input:
        prompt_to_process = user_input

    if prompt_to_process:
        st.session_state.messages.append({"role": "user", "content": prompt_to_process})

        with st.spinner("🧠 Cortex AI is analyzing your request..."):
            response = get_ai_response(st.session_state.messages)

        spec = extract_cohort_spec(response)
        if spec:
            st.session_state.cohort_spec = spec
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.session_state.phase = "review"
        else:
            st.session_state.messages.append({"role": "assistant", "content": response})

        st.experimental_rerun()

elif st.session_state.phase == "review":
    st.caption("Step 2 of 3 — Review the cohort definition, then confirm or refine")

    spec = st.session_state.cohort_spec
    if spec:
        st.subheader(f"📋 {spec.get('cohort_name', 'Cohort Definition')}")

        st.markdown("**Inclusion criteria:**")
        for c in spec.get("inclusion_criteria", []):
            st.markdown(f"- {c.get('description', '')}")

        if spec.get("exclusion_criteria"):
            st.markdown("**Exclusion criteria:**")
            for c in spec.get("exclusion_criteria", []):
                st.markdown(f"- {c.get('description', '')}")

        st.caption(f"Time window: {spec.get('time_window_months', 12)} months")

        with st.expander("🔍 View generated SQL"):
            st.code(build_cohort_sql(spec), language="sql")

        st.markdown("---")
        col_confirm, col_refine = st.columns(2)
        with col_confirm:
            if st.button("✅ Confirm & run cohort", type="primary", use_container_width=True):
                st.session_state.pending_confirm = True
                st.experimental_rerun()
        with col_refine:
            if st.button("✏️ Refine criteria", use_container_width=True):
                st.session_state.pending_refine = True
                st.experimental_rerun()
    else:
        st.warning("No cohort specification found. Please go back and describe your cohort.")
        if st.button("⬅️ Back to step 1"):
            st.session_state.phase = "describe"
            st.experimental_rerun()

elif st.session_state.phase == "results":
    cohort_df = st.session_state.cohort_df
    spec = st.session_state.cohort_spec

    if cohort_df is not None and spec is not None:
        st.success(f"Cohort **{spec.get('cohort_name', '')}** — {len(cohort_df):,} patients found")

        tab_demo, tab_clinical, tab_rwe, tab_hcp, tab_export = st.tabs([
            "📊 Demographics",
            "🔬 Clinical metrics",
            "📈 RWE insights",
            "👨‍⚕️ HCP prioritization",
            "💾 Save & export"
        ])

        with tab_demo:
            render_cohort_metrics(cohort_df)

        with tab_clinical:
            render_clinical_metrics(cohort_df)

        with tab_rwe:
            render_rwe_insights(cohort_df, spec)

        with tab_hcp:
            st.caption("Providers ranked by cohort patient count")
            render_hcp_view(cohort_df)

        with tab_export:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("🤖 AI-generated summary")
                if st.button("Generate summary"):
                    with st.spinner("Generating clinical summary..."):
                        summary = generate_summary(cohort_df, spec)
                    st.session_state["last_summary"] = summary

                if "last_summary" in st.session_state:
                    st.markdown(st.session_state["last_summary"])

            with col2:
                st.subheader("📤 Export options")
                cohort_name = st.text_input("Cohort name",
                    value=spec.get("cohort_name", "my_cohort"))
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
                    cohort_df[["PATIENT_ID"]].to_csv(index=False),
                    f"cohort_{clean_name}.csv",
                    "text/csv",
                )

            st.subheader("Cohort specification")
            st.json(spec)

            if st.session_state.cohort_sql:
                with st.expander("🔍 View executed SQL"):
                    st.code(st.session_state.cohort_sql, language="sql")
    else:
        st.warning("No cohort data available.")
        if st.button("⬅️ Back to step 1"):
            reset_all()
            st.experimental_rerun()
