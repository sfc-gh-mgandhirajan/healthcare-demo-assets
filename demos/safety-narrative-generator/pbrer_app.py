import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="PBRER Medical Writing Assistant",
    page_icon="📋",
    layout="wide"
)

session = get_active_session()

PBRER_SECTIONS = {
    "1": ("Introduction", "Background information, scope, and reporting period"),
    "2": ("Worldwide Marketing Authorization Status", "List of countries where product is approved and changes"),
    "3": ("Actions Taken for Safety Reasons", "Regulatory actions, withdrawals, restrictions during interval"),
    "4": ("Changes to Reference Safety Information", "Updates to Company Core Data Sheet (CCDS)"),
    "5": ("Estimated Exposure and Use Patterns", "Cumulative and interval patient exposure data"),
    "6": ("Data in Summary Tabulations", "Cumulative tabular summaries from clinical trials"),
    "7": ("Summaries of Significant Findings from Clinical Trials", "New efficacy/safety findings from ongoing/completed trials"),
    "8": ("Findings from Non-Interventional Studies", "Observational studies, registries, surveys"),
    "9": ("Information from Other Clinical Trials and Sources", "Compassionate use, literature, other MAH products"),
    "10": ("Non-Clinical Data", "New preclinical safety findings"),
    "11": ("Literature", "Relevant published safety/efficacy information"),
    "12": ("Other Periodic Reports", "Reference to other submitted periodic reports"),
    "13": ("Lack of Efficacy in Controlled Clinical Trials", "Efficacy failures that may impact benefit-risk"),
    "14": ("Late-Breaking Information", "Significant data after data lock point"),
    "15": ("Overview of Signals: New, Ongoing, or Closed", "Comprehensive signal evaluation"),
    "16": ("Signal and Risk Evaluation", "Detailed analysis of identified and potential risks"),
    "17": ("Benefit Evaluation", "Summary of efficacy and effectiveness data"),
    "18": ("Integrated Benefit-Risk Analysis", "Overall benefit-risk balance assessment"),
    "19": ("Conclusions and Actions", "Summary conclusions and recommended actions"),
}

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

def get_case_details(product_name):
    df = session.sql(f"""
        SELECT CASE_ID, PATIENT_AGE, PATIENT_SEX, MEDDRA_PT, MEDDRA_SOC,
               SERIOUSNESS, EVENT_OUTCOME, EVENT_DESCRIPTION, CAUSALITY_ASSESSMENT,
               REPORTER_COUNTRY, REPORT_SOURCE
        FROM HCLS_DEMO.PHARMACOVIGILANCE.ICSR_CASES
        WHERE SUSPECT_DRUG_NAME LIKE '%{product_name}%'
        ORDER BY SERIOUSNESS DESC, REPORT_DATE DESC
    """).collect()
    return [row.as_dict() for row in df]

def generate_pbrer_section(section_num, product_name, summary_data, soc_data, causality_data, case_data):
    
    soc_summary = "\n".join([f"- {s['MEDDRA_SOC']}: {s['MEDDRA_PT']} ({s['CASE_COUNT']} cases, {s['SERIOUS_COUNT']} serious)" for s in soc_data[:10]])
    causality_summary = "\n".join([f"- {c['CAUSALITY_ASSESSMENT']}: {c['CASE_COUNT']} cases ({c['PERCENTAGE']}%)" for c in causality_data])
    countries = list(set([c['REPORTER_COUNTRY'] for c in case_data]))
    
    section_title, section_desc = PBRER_SECTIONS[section_num]
    
    base_context = f"""Product: {product_name}
Reporting Period: July 2024 - December 2024
Data Lock Point: December 31, 2024
Total Cases: {summary_data['TOTAL_CASES']}
Serious Cases: {summary_data['SERIOUS_CASES']}
Non-Serious Cases: {summary_data['NON_SERIOUS_CASES']}
Countries Reporting: {', '.join(countries)}
Spontaneous Reports: {summary_data['SPONTANEOUS_REPORTS']}
Clinical Trial Reports: {summary_data['CLINICAL_TRIAL_REPORTS']}

ADVERSE EVENTS BY SOC:
{soc_summary}

CAUSALITY DISTRIBUTION:
{causality_summary}"""

    prompts = {
        "1": f"""Write PBRER Section 1: Introduction for {product_name}.

{base_context}

Write a formal introduction (200-300 words) following ICH E2C(R2) format that includes:
1. Product identification (active substance, brand name)
2. Brief description of approved indications
3. Data lock point and reporting interval dates
4. Purpose and scope of this PBRER
5. International Birth Date (IBD) reference

Use formal regulatory language appropriate for health authority submission.""",

        "2": f"""Write PBRER Section 2: Worldwide Marketing Authorization Status for {product_name}.

Countries with reported cases: {', '.join(countries)}
Total reporting countries: {summary_data['COUNTRIES_REPORTING']}

Write a concise section (150-250 words) that:
1. Lists regions/countries where the product is authorized
2. Notes any changes in marketing authorization during the reporting period
3. Mentions any pending applications or withdrawals
4. References the annexed table of marketing authorizations

Format as regulatory submission text.""",

        "5": f"""Write PBRER Section 5: Estimated Exposure and Use Patterns for {product_name}.

{base_context}

Write a detailed exposure section (300-400 words) following ICH E2C(R2) that includes:
1. Method of exposure estimation (sales data, prescription data, patient registries)
2. Cumulative exposure since international birth date
3. Interval exposure during this reporting period
4. Patient demographics breakdown (age, gender where available)
5. Geographic distribution of exposure
6. Any notable changes in prescribing patterns

Include quantitative estimates where data allows.""",

        "15": f"""Write PBRER Section 15: Overview of Signals - New, Ongoing, or Closed for {product_name}.

{base_context}

Write a comprehensive signal overview (400-500 words) following ICH E2C(R2) that:
1. Lists any NEW signals identified during this reporting period
2. Describes ONGOING signals under evaluation with current status
3. Documents CLOSED signals with rationale for closure
4. For each signal: source of detection, evaluation status, and actions taken
5. References signal detection methodology used
6. Cross-references to Section 16 for detailed evaluation

Format with clear subsections for New/Ongoing/Closed signals.""",

        "16": f"""Write PBRER Section 16: Signal and Risk Evaluation for {product_name}.

{base_context}

Write a detailed risk evaluation (400-500 words) following ICH E2C(R2) covering:
1. Identified Risks - risks confirmed and included in reference safety information
2. Potential Risks - risks requiring further characterization
3. Missing Information - gaps in safety knowledge
4. For each risk category: characterization, frequency, severity, risk factors
5. Comparison to reference safety information
6. New safety concerns identified during interval

Use MedDRA terminology and include quantitative data where available.""",

        "17": f"""Write PBRER Section 17: Benefit Evaluation for {product_name}.

Product: {product_name}
Approved Indications: Based on therapeutic class
Total Patient Exposure: Derived from {summary_data['TOTAL_CASES']} reported cases

Write a benefit evaluation (300-400 words) following ICH E2C(R2) that:
1. Summarizes approved indications and therapeutic context
2. Reviews efficacy data from clinical trials (pivotal and post-marketing)
3. Discusses effectiveness in real-world clinical practice
4. Notes any new efficacy information from the reporting period
5. Addresses any limitations in benefit data
6. References published literature supporting benefits

Focus on clinically meaningful outcomes.""",

        "18": f"""Write PBRER Section 18: Integrated Benefit-Risk Analysis for {product_name}.

{base_context}

Write a comprehensive integrated benefit-risk analysis (500-600 words) following ICH E2C(R2) that:
1. Summarizes the benefit-risk balance in the context of:
   - Therapeutic context and medical need
   - Available treatment alternatives
2. Weighs documented benefits against identified risks
3. Considers the following framework:
   - Nature and severity of the condition being treated
   - Unmet medical need
   - Efficacy in approved indications
   - Safety profile based on cumulative data
4. Addresses any changes to benefit-risk since last PBRER
5. Discusses populations where benefit-risk may differ
6. Provides an overall conclusion on benefit-risk balance

Use structured benefit-risk assessment methodology.""",

        "19": f"""Write PBRER Section 19: Conclusions and Actions for {product_name}.

{base_context}

Write the conclusions section (300-400 words) following ICH E2C(R2) that:
1. Summarizes overall conclusions from this PBRER
2. States whether the benefit-risk balance remains favorable
3. Lists any ACTIONS PROPOSED OR TAKEN:
   - Changes to reference safety information
   - Changes to product information/labeling
   - Risk minimization measures
   - Additional studies or surveillance
4. Notes any planned regulatory submissions
5. Confirms commitment to ongoing safety monitoring
6. Provides clear regulatory recommendation

End with definitive statement on product's benefit-risk status."""
    }
    
    prompt = prompts.get(section_num, f"Write PBRER Section {section_num}: {section_title} for {product_name}. {base_context}")
    escaped_prompt = prompt.replace("'", "''")
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            '{escaped_prompt}'
        ) AS CONTENT
    """).collect()
    
    return result[0]['CONTENT'] if result else "Error generating content"

st.title("📋 PBRER Medical Writing Assistant")
st.markdown("**ICH E2C(R2) Compliant** | Powered by Snowflake Cortex")

st.divider()

col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("📦 Product Selection")
    
    products = get_products()
    product_options = {f"{p['PRODUCT_NAME']} ({p['ACTIVE_INGREDIENT']})": p['PRODUCT_NAME'] for p in products}
    
    selected_display = st.selectbox("Select Product", options=list(product_options.keys()))
    selected_product = product_options[selected_display]
    
    summary = get_product_summary(selected_product)
    
    if summary:
        st.markdown("---")
        st.markdown("**Interval Summary**")
        st.metric("Total Cases", summary['TOTAL_CASES'])
        
        col_a, col_b = st.columns(2)
        with col_a:
            st.metric("Serious", summary['SERIOUS_CASES'])
        with col_b:
            st.metric("Non-Serious", summary['NON_SERIOUS_CASES'])
        
        st.metric("Countries", summary['COUNTRIES_REPORTING'])
        
        st.markdown("---")
        st.caption("**Report Sources**")
        st.caption(f"Spontaneous: {summary['SPONTANEOUS_REPORTS']}")
        st.caption(f"Clinical Trial: {summary['CLINICAL_TRIAL_REPORTS']}")
        st.caption(f"Literature: {summary['LITERATURE_REPORTS']}")

with col2:
    st.subheader("📝 PBRER Section Generator (ICH E2C(R2))")
    
    st.markdown("**Standard PBRER Structure - 19 Sections**")
    
    soc_data = get_soc_summary(selected_product) if summary else []
    causality_data = get_causality_data(selected_product) if summary else []
    case_data = get_case_details(selected_product) if summary else []
    
    tab_labels = ["Overview", "Intro (§1-4)", "Exposure (§5-6)", "Clinical (§7-14)", "Signals (§15-16)", "B-R Analysis (§17-19)"]
    tabs = st.tabs(tab_labels)
    
    with tabs[0]:
        st.markdown("### ICH E2C(R2) PBRER Structure")
        st.markdown("The Periodic Benefit-Risk Evaluation Report consists of **19 sections**:")
        
        for num, (title, desc) in PBRER_SECTIONS.items():
            st.markdown(f"**Section {num}:** {title}")
            st.caption(f"_{desc}_")
        
        st.info("Select tabs above to generate specific PBRER sections using AI.")
    
    with tabs[1]:
        st.markdown("### Sections 1-4: Introduction & Authorization")
        
        if st.button("Generate Section 1: Introduction", key="btn_s1"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("1", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s1'] = content
        if 'content_s1' in st.session_state:
            st.markdown("**Section 1: Introduction**")
            st.markdown(st.session_state['content_s1'])
            st.download_button("📥 Download", st.session_state['content_s1'], file_name=f"PBRER_S1_{selected_product}.txt", key="d1")
        
        st.divider()
        
        if st.button("Generate Section 2: Worldwide Marketing Authorization Status", key="btn_s2"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("2", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s2'] = content
        if 'content_s2' in st.session_state:
            st.markdown("**Section 2: Worldwide Marketing Authorization Status**")
            st.markdown(st.session_state['content_s2'])
            st.download_button("📥 Download", st.session_state['content_s2'], file_name=f"PBRER_S2_{selected_product}.txt", key="d2")
    
    with tabs[2]:
        st.markdown("### Section 5: Estimated Exposure and Use Patterns")
        
        if st.button("Generate Section 5: Estimated Exposure", key="btn_s5"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("5", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s5'] = content
        if 'content_s5' in st.session_state:
            st.markdown("**Section 5: Estimated Exposure and Use Patterns**")
            st.markdown(st.session_state['content_s5'])
            st.download_button("📥 Download", st.session_state['content_s5'], file_name=f"PBRER_S5_{selected_product}.txt", key="d5")
        
        st.divider()
        st.markdown("**Exposure Data Summary (from database)**")
        if summary:
            exp_data = {
                "Metric": ["Total Cases Reported", "Spontaneous Reports", "Clinical Trial Reports", "Unique Countries"],
                "Value": [summary['TOTAL_CASES'], summary['SPONTANEOUS_REPORTS'], summary['CLINICAL_TRIAL_REPORTS'], summary['COUNTRIES_REPORTING']]
            }
            st.dataframe(exp_data, use_container_width=True)
    
    with tabs[3]:
        st.markdown("### Sections 7-14: Clinical Data & Findings")
        st.markdown("**Adverse Events by System Organ Class**")
        
        if soc_data:
            soc_display = [{
                "SOC": s['MEDDRA_SOC'],
                "PT": s['MEDDRA_PT'],
                "Total": s['CASE_COUNT'],
                "Serious": s['SERIOUS_COUNT'],
                "Recovered": s['RECOVERED']
            } for s in soc_data]
            st.dataframe(soc_display, use_container_width=True)
        
        st.markdown("**Causality Assessment Distribution**")
        if causality_data:
            caus_display = [{
                "Assessment": c['CAUSALITY_ASSESSMENT'],
                "Cases": c['CASE_COUNT'],
                "%": f"{c['PERCENTAGE']}%"
            } for c in causality_data]
            st.dataframe(caus_display, use_container_width=True)
    
    with tabs[4]:
        st.markdown("### Sections 15-16: Signal Evaluation")
        
        if st.button("Generate Section 15: Overview of Signals", key="btn_s15"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("15", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s15'] = content
        if 'content_s15' in st.session_state:
            st.markdown("**Section 15: Overview of Signals**")
            st.markdown(st.session_state['content_s15'])
            st.download_button("📥 Download", st.session_state['content_s15'], file_name=f"PBRER_S15_{selected_product}.txt", key="d15")
        
        st.divider()
        
        if st.button("Generate Section 16: Signal and Risk Evaluation", key="btn_s16"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("16", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s16'] = content
        if 'content_s16' in st.session_state:
            st.markdown("**Section 16: Signal and Risk Evaluation**")
            st.markdown(st.session_state['content_s16'])
            st.download_button("📥 Download", st.session_state['content_s16'], file_name=f"PBRER_S16_{selected_product}.txt", key="d16")
    
    with tabs[5]:
        st.markdown("### Sections 17-19: Benefit-Risk Analysis & Conclusions")
        
        if st.button("Generate Section 17: Benefit Evaluation", key="btn_s17"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("17", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s17'] = content
        if 'content_s17' in st.session_state:
            st.markdown("**Section 17: Benefit Evaluation**")
            st.markdown(st.session_state['content_s17'])
            st.download_button("📥 Download", st.session_state['content_s17'], file_name=f"PBRER_S17_{selected_product}.txt", key="d17")
        
        st.divider()
        
        if st.button("Generate Section 18: Integrated Benefit-Risk Analysis", type="primary", key="btn_s18"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("18", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s18'] = content
        if 'content_s18' in st.session_state:
            st.markdown("**Section 18: Integrated Benefit-Risk Analysis**")
            st.markdown(st.session_state['content_s18'])
            st.download_button("📥 Download", st.session_state['content_s18'], file_name=f"PBRER_S18_{selected_product}.txt", key="d18")
        
        st.divider()
        
        if st.button("Generate Section 19: Conclusions and Actions", key="btn_s19"):
            with st.spinner("Generating..."):
                content = generate_pbrer_section("19", selected_product, summary, soc_data, causality_data, case_data)
                st.session_state['content_s19'] = content
        if 'content_s19' in st.session_state:
            st.markdown("**Section 19: Conclusions and Actions**")
            st.markdown(st.session_state['content_s19'])
            st.download_button("📥 Download", st.session_state['content_s19'], file_name=f"PBRER_S19_{selected_product}.txt", key="d19")

st.divider()
col_f1, col_f2, col_f3, col_f4 = st.columns(4)
with col_f1:
    st.metric("Standard", "ICH E2C(R2)")
with col_f2:
    st.metric("Sections", "19")
with col_f3:
    st.metric("Model", "Llama 3.1 70B")
with col_f4:
    st.metric("Governance", "In-Snowflake")

st.caption("PBRER Medical Writing Assistant | Aligned with ICH E2C(R2) Periodic Benefit-Risk Evaluation Report Guidelines")
