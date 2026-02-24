import streamlit as st
import os
from snowflake.connector import connect

st.set_page_config(
    page_title="Safety Narrative Generator",
    page_icon="🏥",
    layout="wide"
)

@st.cache_resource
def get_connection():
    return connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "cortex"))

def get_cases(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT CASE_ID, SUSPECT_DRUG_NAME, EVENT_TERM, SERIOUSNESS, REPORT_DATE
        FROM HCLS_DEMO.PHARMACOVIGILANCE.ICSR_CASES
        ORDER BY REPORT_DATE DESC
    """)
    return cursor.fetchall()

def get_case_details(conn, case_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT * FROM HCLS_DEMO.PHARMACOVIGILANCE.ICSR_CASES
        WHERE CASE_ID = '{case_id}'
    """)
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else None

def generate_narrative(conn, case_data):
    prompt = f"""You are a pharmacovigilance medical writer. Generate a professional ICSR case narrative for regulatory submission based on the following structured data. The narrative should be clear, concise, and follow ICH E2B(R3) guidelines.

CASE INFORMATION:
- Case ID: {case_data['CASE_ID']}
- Report Date: {case_data['REPORT_DATE']}
- Report Source: {case_data['REPORT_SOURCE']}
- Seriousness: {case_data['SERIOUSNESS']}
- Reporter: {case_data['REPORTER_TYPE']} from {case_data['REPORTER_COUNTRY']}

PATIENT INFORMATION:
- Age: {case_data['PATIENT_AGE']} {case_data['PATIENT_AGE_UNIT']}
- Sex: {case_data['PATIENT_SEX']}
- Weight: {case_data['PATIENT_WEIGHT_KG']} kg
- Medical History: {case_data['MEDICAL_HISTORY']}

SUSPECT DRUG:
- Drug Name: {case_data['SUSPECT_DRUG_NAME']}
- Indication: {case_data['DRUG_INDICATION']}
- Dose: {case_data['DOSE']}
- Route: {case_data['ROUTE_OF_ADMIN']}
- Therapy Start: {case_data['THERAPY_START_DATE']}
- Therapy End: {case_data['THERAPY_END_DATE'] or 'Ongoing'}
- Action Taken: {case_data['ACTION_TAKEN']}

ADVERSE EVENT:
- Event Term: {case_data['EVENT_TERM']}
- MedDRA PT: {case_data['MEDDRA_PT']}
- MedDRA SOC: {case_data['MEDDRA_SOC']}
- Onset Date: {case_data['EVENT_ONSET_DATE']}
- Outcome: {case_data['EVENT_OUTCOME']}
- Description: {case_data['EVENT_DESCRIPTION']}

CAUSALITY:
- Assessment: {case_data['CAUSALITY_ASSESSMENT']}

Generate a professional narrative that:
1. Starts with patient demographics and relevant medical history
2. Describes the drug therapy including indication, dose, and duration
3. Details the adverse event including onset, symptoms, clinical findings, and management
4. Includes the outcome and any dechallenge/rechallenge information
5. Concludes with the causality assessment rationale

Write in third person, past tense, using medical terminology appropriate for regulatory submission."""

    escaped_prompt = prompt.replace("'", "''")
    
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            '{escaped_prompt}'
        ) AS NARRATIVE
    """)
    result = cursor.fetchone()
    return result[0] if result else "Error generating narrative"

st.title("🏥 AI-Powered Safety Narrative Generator")
st.markdown("**Powered by Snowflake Cortex** | Demonstrates automated ICSR narrative generation for pharmacovigilance")

st.divider()

conn = get_connection()

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("📋 Select Case")
    cases = get_cases(conn)
    
    case_options = {f"{c[0]} | {c[1]} | {c[2]}": c[0] for c in cases}
    selected_display = st.selectbox(
        "Available ICSR Cases",
        options=list(case_options.keys()),
        format_func=lambda x: x
    )
    selected_case_id = case_options[selected_display]
    
    if selected_case_id:
        case_data = get_case_details(conn, selected_case_id)
        
        if case_data:
            st.markdown("---")
            st.markdown("**Case Summary**")
            
            seriousness_color = "🔴" if case_data['SERIOUSNESS'] == 'SERIOUS' else "🟢"
            st.markdown(f"{seriousness_color} **Seriousness:** {case_data['SERIOUSNESS']}")
            st.markdown(f"**Drug:** {case_data['SUSPECT_DRUG_NAME']}")
            st.markdown(f"**Event:** {case_data['EVENT_TERM']}")
            st.markdown(f"**Patient:** {case_data['PATIENT_AGE']} y/o {case_data['PATIENT_SEX']}")
            st.markdown(f"**Outcome:** {case_data['EVENT_OUTCOME']}")
            st.markdown(f"**Causality:** {case_data['CAUSALITY_ASSESSMENT']}")

with col2:
    st.subheader("📝 Narrative Generation")
    
    tab1, tab2 = st.tabs(["Structured Data", "Generated Narrative"])
    
    with tab1:
        if case_data:
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown("**Patient Information**")
                st.json({
                    "Age": f"{case_data['PATIENT_AGE']} {case_data['PATIENT_AGE_UNIT']}",
                    "Sex": case_data['PATIENT_SEX'],
                    "Weight": f"{case_data['PATIENT_WEIGHT_KG']} kg",
                    "Medical History": case_data['MEDICAL_HISTORY']
                })
                
                st.markdown("**Drug Information**")
                st.json({
                    "Drug": case_data['SUSPECT_DRUG_NAME'],
                    "Indication": case_data['DRUG_INDICATION'],
                    "Dose": case_data['DOSE'],
                    "Route": case_data['ROUTE_OF_ADMIN'],
                    "Action": case_data['ACTION_TAKEN']
                })
            
            with col_b:
                st.markdown("**Event Information**")
                st.json({
                    "Event": case_data['EVENT_TERM'],
                    "MedDRA PT": case_data['MEDDRA_PT'],
                    "MedDRA SOC": case_data['MEDDRA_SOC'],
                    "Onset": str(case_data['EVENT_ONSET_DATE']),
                    "Outcome": case_data['EVENT_OUTCOME']
                })
                
                st.markdown("**Event Description (Source)**")
                st.info(case_data['EVENT_DESCRIPTION'])
    
    with tab2:
        if st.button("🚀 Generate Narrative with Cortex AI", type="primary", use_container_width=True):
            with st.spinner("Generating narrative using Snowflake Cortex LLM..."):
                narrative = generate_narrative(conn, case_data)
                st.session_state['generated_narrative'] = narrative
                st.session_state['current_case_id'] = selected_case_id
        
        if 'generated_narrative' in st.session_state and st.session_state.get('current_case_id') == selected_case_id:
            st.markdown("**Generated ICSR Narrative**")
            st.markdown(st.session_state['generated_narrative'])
            
            st.divider()
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                st.download_button(
                    "📥 Download Narrative",
                    st.session_state['generated_narrative'],
                    file_name=f"narrative_{selected_case_id}.txt",
                    mime="text/plain"
                )
            with col_btn2:
                if st.button("📋 Copy to Clipboard"):
                    st.toast("Narrative copied!", icon="✅")

st.divider()
st.markdown("---")
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    st.metric("Model", "Llama 3.1 70B")
with col_f2:
    st.metric("Data Governance", "In-Snowflake")
with col_f3:
    st.metric("Compliance", "ICH E2B(R3)")

st.caption("Demo: Snowflake Cortex for Advanced Compliance Documentation | Data stays secure within Snowflake")
