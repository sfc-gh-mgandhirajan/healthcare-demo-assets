import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

st.set_page_config(
    page_title="PSUR Medical Writing Assistant",
    page_icon="📋",
    layout="wide"
)

session = get_active_session()

def get_products():
    df = session.sql("""
        SELECT PRODUCT_ID, PRODUCT_NAME, ACTIVE_INGREDIENT, THERAPEUTIC_CLASS, 
               LAST_PSUR_DATE, NEXT_PSUR_DUE
        FROM HCLS_DEMO.PHARMACOVIGILANCE.PRODUCT_REGISTRY
        ORDER BY PRODUCT_NAME
    """).collect()
    return df

def get_product_summary(product_name):
    df = session.sql(f"""
        SELECT * FROM HCLS_DEMO.PHARMACOVIGILANCE.V_PSUR_PRODUCT_SUMMARY
        WHERE PRODUCT LIKE '%{product_name}%'
    """).collect()
    return df[0].as_dict() if df else None

def get_soc_summary(product_name):
    df = session.sql(f"""
        SELECT * FROM HCLS_DEMO.PHARMACOVIGILANCE.V_PSUR_CASES_BY_SOC
        WHERE PRODUCT LIKE '%{product_name}%'
        ORDER BY CASE_COUNT DESC
    """).collect()
    return [row.as_dict() for row in df]

def get_causality_data(product_name):
    df = session.sql(f"""
        SELECT * FROM HCLS_DEMO.PHARMACOVIGILANCE.V_PSUR_CAUSALITY
        WHERE PRODUCT LIKE '%{product_name}%'
    """).collect()
    return [row.as_dict() for row in df]

def get_case_narratives(product_name):
    df = session.sql(f"""
        SELECT CASE_ID, PATIENT_AGE, PATIENT_SEX, MEDDRA_PT, MEDDRA_SOC,
               SERIOUSNESS, EVENT_OUTCOME, EVENT_DESCRIPTION, CAUSALITY_ASSESSMENT
        FROM HCLS_DEMO.PHARMACOVIGILANCE.ICSR_CASES
        WHERE SUSPECT_DRUG_NAME LIKE '%{product_name}%'
        ORDER BY SERIOUSNESS DESC, REPORT_DATE DESC
    """).collect()
    return [row.as_dict() for row in df]

def generate_psur_section(section_type, product_name, summary_data, soc_data, causality_data, case_data):
    
    soc_summary = "\n".join([f"- {s['MEDDRA_SOC']}: {s['MEDDRA_PT']} ({s['CASE_COUNT']} cases, {s['SERIOUS_COUNT']} serious)" for s in soc_data[:10]])
    causality_summary = "\n".join([f"- {c['CAUSALITY_ASSESSMENT']}: {c['CASE_COUNT']} cases ({c['PERCENTAGE']}%)" for c in causality_data])
    
    prompts = {
        "executive_summary": f"""You are a pharmacovigilance medical writer preparing a PSUR (Periodic Safety Update Report) for regulatory submission. 
Write an Executive Summary section for {product_name} based on this safety data:

REPORTING PERIOD DATA:
- Total Cases: {summary_data['TOTAL_CASES']}
- Serious Cases: {summary_data['SERIOUS_CASES']}
- Non-Serious Cases: {summary_data['NON_SERIOUS_CASES']}
- Countries Reporting: {summary_data['COUNTRIES_REPORTING']}
- Spontaneous Reports: {summary_data['SPONTANEOUS_REPORTS']}
- Clinical Trial Reports: {summary_data['CLINICAL_TRIAL_REPORTS']}
- Unique SOCs Affected: {summary_data['UNIQUE_SOCS']}
- Unique Preferred Terms: {summary_data['UNIQUE_PTS']}

TOP ADVERSE EVENTS BY SOC:
{soc_summary}

CAUSALITY DISTRIBUTION:
{causality_summary}

Write a professional executive summary (300-400 words) that:
1. Summarizes the overall safety profile during this reporting period
2. Highlights key findings and any new safety signals
3. Provides a brief benefit-risk statement
4. Follows ICH E2C(R2) PSUR guidance format""",

        "safety_evaluation": f"""You are a pharmacovigilance medical writer. Write the Safety Evaluation section of a PSUR for {product_name}.

CUMULATIVE SAFETY DATA:
- Total Cases Received: {summary_data['TOTAL_CASES']}
- Serious Cases: {summary_data['SERIOUS_CASES']} ({round(summary_data['SERIOUS_CASES']*100/summary_data['TOTAL_CASES'], 1)}%)
- High Causality Cases (Certain/Probable): {summary_data['HIGH_CAUSALITY_CASES']}

ADVERSE EVENTS BY SYSTEM ORGAN CLASS:
{soc_summary}

CAUSALITY ASSESSMENT SUMMARY:
{causality_summary}

Write a comprehensive safety evaluation (400-500 words) that:
1. Analyzes the safety data by System Organ Class
2. Discusses serious vs non-serious case distribution
3. Evaluates causality patterns
4. Identifies any emerging safety signals or trends
5. Compares to the known safety profile from the product label
6. Follows regulatory medical writing standards""",

        "benefit_risk": f"""You are a pharmacovigilance medical writer. Write the Benefit-Risk Analysis section of a PSUR for {product_name}.

PRODUCT INFORMATION:
- Product: {product_name}
- Therapeutic Class: Based on indication data

SAFETY SUMMARY:
- Total Cases: {summary_data['TOTAL_CASES']}
- Serious Cases: {summary_data['SERIOUS_CASES']}
- Most Common AEs: {', '.join([s['MEDDRA_PT'] for s in soc_data[:5]])}

CAUSALITY:
{causality_summary}

Write a benefit-risk analysis (300-400 words) that:
1. Summarizes the known benefits of the product for approved indications
2. Weighs the benefits against the observed safety profile
3. Discusses risk mitigation measures in place
4. Provides an overall benefit-risk conclusion
5. Recommends any label updates if warranted
6. Follows ICH E2C(R2) format requirements""",

        "signal_evaluation": f"""You are a pharmacovigilance medical writer. Write a Signal Evaluation section for a PSUR for {product_name}.

ADVERSE EVENT DATA BY PREFERRED TERM:
{soc_summary}

SERIOUS CASES: {summary_data['SERIOUS_CASES']} out of {summary_data['TOTAL_CASES']} total

HIGH CAUSALITY CASES: {summary_data['HIGH_CAUSALITY_CASES']}

Write a signal evaluation (300-400 words) that:
1. Identifies potential new safety signals based on the data
2. Evaluates known safety signals and their status (ongoing, closed, new)
3. Discusses any disproportionality in reporting
4. Recommends signal-related actions if needed
5. References relevant case series or clusters
6. Follows pharmacovigilance signal detection best practices"""
    }
    
    prompt = prompts.get(section_type, prompts["executive_summary"])
    escaped_prompt = prompt.replace("'", "''")
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            '{escaped_prompt}'
        ) AS CONTENT
    """).collect()
    
    return result[0]['CONTENT'] if result else "Error generating content"

st.title("📋 PSUR Medical Writing Assistant")
st.markdown("**Powered by Snowflake Cortex** | AI-assisted aggregate report generation for pharmacovigilance")

st.divider()

col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("📦 Select Product")
    
    products = get_products()
    product_options = {f"{p['PRODUCT_NAME']} ({p['ACTIVE_INGREDIENT']})": p['PRODUCT_NAME'] for p in products}
    
    selected_display = st.selectbox("Product", options=list(product_options.keys()))
    selected_product = product_options[selected_display]
    
    summary = get_product_summary(selected_product)
    
    if summary:
        st.markdown("---")
        st.markdown("**Safety Data Summary**")
        st.metric("Total Cases", summary['TOTAL_CASES'])
        
        col_a, col_b = st.columns(2)
        with col_a:
            st.metric("Serious", summary['SERIOUS_CASES'], delta=None, delta_color="inverse")
        with col_b:
            st.metric("Non-Serious", summary['NON_SERIOUS_CASES'])
        
        st.metric("Countries", summary['COUNTRIES_REPORTING'])
        st.metric("Unique SOCs", summary['UNIQUE_SOCS'])
        
        st.markdown("---")
        st.markdown("**Report Sources**")
        st.caption(f"Spontaneous: {summary['SPONTANEOUS_REPORTS']}")
        st.caption(f"Clinical Trial: {summary['CLINICAL_TRIAL_REPORTS']}")
        st.caption(f"Literature: {summary['LITERATURE_REPORTS']}")

with col2:
    st.subheader("📝 PSUR Section Generator")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Data Overview", 
        "Executive Summary", 
        "Safety Evaluation",
        "Benefit-Risk",
        "Signal Evaluation"
    ])
    
    soc_data = get_soc_summary(selected_product) if summary else []
    causality_data = get_causality_data(selected_product) if summary else []
    case_data = get_case_narratives(selected_product) if summary else []
    
    with tab1:
        if summary:
            st.markdown("**Adverse Events by System Organ Class**")
            
            soc_display = []
            for s in soc_data:
                soc_display.append({
                    "SOC": s['MEDDRA_SOC'],
                    "Preferred Term": s['MEDDRA_PT'],
                    "Total": s['CASE_COUNT'],
                    "Serious": s['SERIOUS_COUNT'],
                    "Recovered": s['RECOVERED']
                })
            st.dataframe(soc_display, use_container_width=True)
            
            st.markdown("**Causality Assessment Distribution**")
            causality_display = []
            for c in causality_data:
                causality_display.append({
                    "Assessment": c['CAUSALITY_ASSESSMENT'],
                    "Cases": c['CASE_COUNT'],
                    "Percentage": f"{c['PERCENTAGE']}%"
                })
            st.dataframe(causality_display, use_container_width=True)
    
    with tab2:
        st.markdown("**Executive Summary Section**")
        st.caption("ICH E2C(R2) compliant summary of safety data for the reporting period")
        
        if st.button("🚀 Generate Executive Summary", type="primary", key="exec"):
            with st.spinner("Generating executive summary with Cortex AI..."):
                content = generate_psur_section("executive_summary", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['exec_summary'] = content
        
        if 'exec_summary' in st.session_state:
            st.markdown(st.session_state['exec_summary'])
            st.download_button("📥 Download", st.session_state['exec_summary'], 
                             file_name=f"PSUR_Executive_Summary_{selected_product}.txt")
    
    with tab3:
        st.markdown("**Safety Evaluation Section**")
        st.caption("Comprehensive analysis of adverse events by SOC and causality")
        
        if st.button("🚀 Generate Safety Evaluation", type="primary", key="safety"):
            with st.spinner("Generating safety evaluation with Cortex AI..."):
                content = generate_psur_section("safety_evaluation", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['safety_eval'] = content
        
        if 'safety_eval' in st.session_state:
            st.markdown(st.session_state['safety_eval'])
            st.download_button("📥 Download", st.session_state['safety_eval'],
                             file_name=f"PSUR_Safety_Evaluation_{selected_product}.txt")
    
    with tab4:
        st.markdown("**Benefit-Risk Analysis Section**")
        st.caption("Integrated assessment of benefits vs observed safety profile")
        
        if st.button("🚀 Generate Benefit-Risk Analysis", type="primary", key="benefit"):
            with st.spinner("Generating benefit-risk analysis with Cortex AI..."):
                content = generate_psur_section("benefit_risk", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['benefit_risk'] = content
        
        if 'benefit_risk' in st.session_state:
            st.markdown(st.session_state['benefit_risk'])
            st.download_button("📥 Download", st.session_state['benefit_risk'],
                             file_name=f"PSUR_Benefit_Risk_{selected_product}.txt")
    
    with tab5:
        st.markdown("**Signal Evaluation Section**")
        st.caption("Assessment of new and ongoing safety signals")
        
        if st.button("🚀 Generate Signal Evaluation", type="primary", key="signal"):
            with st.spinner("Generating signal evaluation with Cortex AI..."):
                content = generate_psur_section("signal_evaluation", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['signal_eval'] = content
        
        if 'signal_eval' in st.session_state:
            st.markdown(st.session_state['signal_eval'])
            st.download_button("📥 Download", st.session_state['signal_eval'],
                             file_name=f"PSUR_Signal_Evaluation_{selected_product}.txt")

st.divider()
col_f1, col_f2, col_f3, col_f4 = st.columns(4)
with col_f1:
    st.metric("Model", "Llama 3.1 70B")
with col_f2:
    st.metric("Standard", "ICH E2C(R2)")
with col_f3:
    st.metric("Data Source", "In-Snowflake")
with col_f4:
    st.metric("Governance", "Enterprise")

st.caption("Demo: Snowflake Cortex for PSUR Medical Writing | Aggregate Safety Report Generation")
