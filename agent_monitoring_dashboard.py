import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

st.set_page_config(
    page_title="Agent Monitoring Dashboard",
    page_icon="🏥",
    layout="wide"
)

AGENT_NAME = "CHATWITHPATIENTDATA"
AGENT_DATABASE = "SNOWFLAKE_INTELLIGENCE"
AGENT_SCHEMA = "AGENTS"
AGENT_FQN = f"{AGENT_DATABASE}.{AGENT_SCHEMA}.{AGENT_NAME}"

# Demo mode toggle
demo_mode = st.sidebar.toggle("Demo Mode (Sample Data)", value=True)

if not demo_mode:
    try:
        from snowflake.snowpark import Session
        import os
        
        @st.cache_resource
        def get_session():
            return Session.builder.configs({"account": os.getenv("SNOWFLAKE_ACCOUNT"), 
                                           "user": os.getenv("SNOWFLAKE_USER"),
                                           "authenticator": "externalbrowser"}).create()
        session = get_session()
        
        def run_query(query):
            return session.sql(query).to_pandas()
    except Exception as e:
        st.sidebar.error(f"Connection failed: {e}")
        demo_mode = True
        st.sidebar.info("Falling back to Demo Mode")

def generate_sample_usage_data(days=7):
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    return pd.DataFrame({
        'USAGE_DATE': dates,
        'TOTAL_REQUESTS': np.random.randint(15, 85, days),
        'UNIQUE_USERS': np.random.randint(3, 12, days),
        'TOTAL_TOKENS': np.random.randint(5000, 25000, days),
        'TOTAL_CREDITS': np.random.uniform(0.5, 3.5, days).round(2),
        'AVG_LATENCY_SEC': np.random.uniform(2.5, 8.5, days).round(1)
    })

def generate_sample_user_data():
    return pd.DataFrame({
        'USER_NAME': ['CARE_MANAGER_1', 'CLINICIAN_SMITH', 'POP_HEALTH_ANALYST', 'ADMIN_USER', 'DATA_SCIENTIST'],
        'REQUESTS': [156, 89, 67, 45, 32],
        'CREDITS': [12.45, 7.23, 5.67, 3.89, 2.56],
        'LAST_USED': pd.to_datetime(['2026-03-03', '2026-03-03', '2026-03-02', '2026-03-01', '2026-02-28'])
    })

st.title("Agent Governance Dashboard")
st.caption(f"Monitoring **{AGENT_NAME}** | Healthcare Patient Data Agent")

tab1, tab2, tab3 = st.tabs(["🛡️ Guardrails", "👁️ Observability", "📊 Evaluations"])

# =============================================================================
# TAB 1: GUARDRAILS
# =============================================================================
with tab1:
    st.header("Guardrails")
    st.markdown("Preventive controls that protect data and limit risk **before** issues occur.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container(border=True):
            st.subheader("Access Control (RBAC)")
            
            if demo_mode:
                grants_df = pd.DataFrame({
                    'ROLE_NAME': ['CARE_MANAGER_ROLE', 'CLINICIAN_ROLE', 'POP_HEALTH_ROLE', 'ANALYST_ROLE'],
                    'PRIVILEGE': ['USAGE', 'USAGE', 'USAGE', 'USAGE'],
                    'OBJECT_TYPE': ['AGENT', 'AGENT', 'AGENT', 'AGENT'],
                    'GRANTED_BY': ['ACCOUNTADMIN', 'ACCOUNTADMIN', 'ACCOUNTADMIN', 'ACCOUNTADMIN']
                })
                st.dataframe(grants_df, hide_index=True, use_container_width=True)
            else:
                grants_query = f"""
                    SELECT GRANTEE_NAME AS role_name, PRIVILEGE_TYPE AS privilege, GRANTED_ON AS object_type, GRANTED_BY
                    FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
                    WHERE NAME = '{AGENT_NAME}' AND GRANTED_ON = 'AGENT' AND DELETED_ON IS NULL
                    LIMIT 20
                """
                grants_df = run_query(grants_query)
                if not grants_df.empty:
                    st.dataframe(grants_df, hide_index=True, use_container_width=True)
                else:
                    st.info("No explicit grants found.")
            
            st.code(f"""-- Grant agent access to roles
GRANT USAGE ON AGENT {AGENT_FQN} TO ROLE <role_name>;""", language="sql")
    
    with col2:
        with st.container(border=True):
            st.subheader("Data Protection Policies")
            
            if demo_mode:
                policies_df = pd.DataFrame({
                    'POLICY_NAME': ['PHI_MASK', 'PATIENT_ID_MASK', 'DEPT_ROW_ACCESS'],
                    'POLICY_KIND': ['MASKING_POLICY', 'MASKING_POLICY', 'ROW_ACCESS_POLICY'],
                    'APPLIED_TO': ['CLAIMS.PATIENT_DETAILS', 'CLAIMS.PATIENT_DETAILS', 'CLAIMS.CLAIMS_DATA'],
                    'COLUMN_NAME': ['SSN', 'PATIENT_ID', None]
                })
                st.dataframe(policies_df, hide_index=True, use_container_width=True)
            else:
                masking_query = """
                    SELECT POLICY_NAME, POLICY_KIND, 
                           REF_DATABASE_NAME || '.' || REF_SCHEMA_NAME || '.' || REF_ENTITY_NAME AS applied_to,
                           REF_COLUMN_NAME AS column_name
                    FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
                    WHERE POLICY_KIND IN ('MASKING_POLICY', 'ROW_ACCESS_POLICY')
                    LIMIT 20
                """
                policies_df = run_query(masking_query)
                if not policies_df.empty:
                    st.dataframe(policies_df, hide_index=True, use_container_width=True)
                else:
                    st.warning("No policies detected.")
            
            st.caption("Masking & RAP policies automatically apply to agent queries.")
    
    with st.container(border=True):
        st.subheader("Resource & Cost Controls")
        
        col_a, col_b, col_c, col_d = st.columns(4)
        
        with col_a:
            st.metric("Warehouse", "BI_WH", help="Warehouse for Cortex Analyst")
        with col_b:
            st.metric("Query Timeout", "60 sec", help="Max query execution time")
        with col_c:
            st.metric("Max Results", "4", help="Cortex Search max results per tool")
        with col_d:
            st.metric("Resource Monitor", "AI_BUDGET", help="Credit limit enforcement")
    
    with st.container(border=True):
        st.subheader("Agent Tools & Data Sources")
        
        tools_data = pd.DataFrame([
            {"Tool": "patient_claims", "Type": "Cortex Analyst", "Source": "Semantic Model (Claims Data)", "Warehouse": "BI_WH", "Timeout": "60s"},
            {"Tool": "medtranscripts", "Type": "Cortex Search", "Source": "UNSTRUCTURED_HEALTHDATA.MED_TRANSCRIPTS", "Warehouse": "-", "Timeout": "-"},
            {"Tool": "pubmed", "Type": "Cortex Search", "Source": "PUBMED_BIOMEDICAL_RESEARCH_CORPUS", "Warehouse": "-", "Timeout": "-"},
        ])
        st.dataframe(tools_data, hide_index=True, use_container_width=True)

# =============================================================================
# TAB 2: OBSERVABILITY
# =============================================================================
with tab2:
    st.header("Observability")
    st.markdown("Real-time visibility into agent usage, performance, and conversations.")
    
    days_back = st.selectbox("Time Range", [7, 14, 30], index=0, format_func=lambda x: f"Last {x} days")
    
    if demo_mode:
        usage_df = generate_sample_usage_data(days_back)
    else:
        usage_query = f"""
            SELECT DATE_TRUNC('day', START_TIME) AS usage_date,
                   COUNT(*) AS total_requests, COUNT(DISTINCT USER_NAME) AS unique_users,
                   SUM(TOKENS) AS total_tokens, SUM(TOKEN_CREDITS) AS total_credits,
                   AVG(DATEDIFF('second', START_TIME, END_TIME)) AS avg_latency_sec
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
            WHERE AGENT_NAME = '{AGENT_NAME}' AND START_TIME >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('day', START_TIME) ORDER BY usage_date DESC
        """
        usage_df = run_query(usage_query)
    
    if not usage_df.empty:
        with st.container(horizontal=True):
            total_requests = int(usage_df['TOTAL_REQUESTS'].sum())
            total_credits = float(usage_df['TOTAL_CREDITS'].sum())
            total_users = int(usage_df['UNIQUE_USERS'].max())
            avg_latency = float(usage_df['AVG_LATENCY_SEC'].mean())
            
            st.metric("Total Requests", f"{total_requests:,}", delta="+12%", border=True)
            st.metric("Total Credits", f"{total_credits:.2f}", delta="-5%", border=True)
            st.metric("Unique Users", f"{total_users}", delta="+2", border=True)
            st.metric("Avg Latency", f"{avg_latency:.1f}s", delta="-0.3s", border=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container(border=True):
                st.subheader("Daily Request Volume")
                st.bar_chart(usage_df, x='USAGE_DATE', y='TOTAL_REQUESTS', color="#1f77b4")
        
        with col2:
            with st.container(border=True):
                st.subheader("Daily Credit Consumption")
                st.line_chart(usage_df, x='USAGE_DATE', y='TOTAL_CREDITS', color="#ff7f0e")
    else:
        st.info(f"No usage data found for {AGENT_NAME} in the last {days_back} days.")
    
    with st.container(border=True):
        st.subheader("Usage by User")
        
        if demo_mode:
            user_df = generate_sample_user_data()
        else:
            user_query = f"""
                SELECT USER_NAME, COUNT(*) AS requests, SUM(TOKEN_CREDITS) AS credits, MAX(START_TIME) AS last_used
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
                WHERE AGENT_NAME = '{AGENT_NAME}' AND START_TIME >= DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
                GROUP BY USER_NAME ORDER BY requests DESC LIMIT 10
            """
            user_df = run_query(user_query)
        
        if not user_df.empty:
            st.dataframe(user_df, hide_index=True, use_container_width=True)
    
    with st.container(border=True):
        st.subheader("Conversation Traces")
        st.markdown("View detailed execution traces for debugging and auditing.")
        
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**What's Captured:**")
            st.markdown("""
            - LLM planning & reasoning steps
            - Tool selection decisions
            - SQL queries generated (Cortex Analyst)
            - Search results retrieved (Cortex Search)
            - Final response generation
            - User feedback (thumbs up/down)
            """)
        
        with col_b:
            st.code(f"""-- Query conversation traces
SELECT * FROM TABLE(
  SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    '{AGENT_DATABASE}', '{AGENT_SCHEMA}', 
    '{AGENT_NAME}', 'CORTEX AGENT'
  )
) ORDER BY TIMESTAMP DESC LIMIT 50;""", language="sql")
        
        st.info("Access traces via Snowsight: **AI & ML → Agents → ChatWithPatientData → Monitoring**")

# =============================================================================
# TAB 3: EVALUATIONS
# =============================================================================
with tab3:
    st.header("Evaluations")
    st.markdown("Systematic measurement of AI quality, accuracy, and reliability.")
    
    with st.container(border=True):
        st.subheader("Evaluation Metrics")
        st.markdown("Use **AI Observability** with TruLens to measure these key metrics:")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Context Relevance", "0.87", delta="+0.03", help="Are retrieved docs relevant?", border=True)
        with col2:
            st.metric("Answer Relevance", "0.91", delta="+0.02", help="Does response address query?", border=True)
        with col3:
            st.metric("Groundedness", "0.94", delta="+0.01", help="Is response factually grounded?", border=True)
        with col4:
            st.metric("Avg Latency", "4.2s", delta="-0.5s", help="Response time", border=True)
        with col5:
            st.metric("Cost/Query", "$0.32", delta="-$0.05", help="Credits per request", border=True)
        
        metrics_detail = pd.DataFrame([
            {"Metric": "Context Relevance", "Description": "Are retrieved documents (transcripts, PubMed) relevant to the query?", "Target": "≥ 0.8", "Current": "0.87"},
            {"Metric": "Answer Relevance", "Description": "Does the response address the user's clinical question?", "Target": "≥ 0.85", "Current": "0.91"},
            {"Metric": "Groundedness", "Description": "Is the response grounded in facts (no hallucination)?", "Target": "≥ 0.9", "Current": "0.94"},
        ])
        st.dataframe(metrics_detail, hide_index=True, use_container_width=True)
    
    with st.container(border=True):
        st.subheader("Sample Evaluation Dataset")
        st.markdown("Test questions for systematic evaluation:")
        
        eval_questions = pd.DataFrame([
            {"Category": "Population Health", "Question": "Where do claim data indicate gaps in recommended screening?", "Expected Tool": "patient_claims + medtranscripts", "Status": "✅ Pass"},
            {"Category": "Readmissions", "Question": "For patients readmitted within 30 days, what reasons are found?", "Expected Tool": "All 3 tools", "Status": "✅ Pass"},
            {"Category": "Imaging", "Question": "Patients who had image guided needle core biopsy?", "Expected Tool": "patient_claims", "Status": "✅ Pass"},
            {"Category": "Patient Summary", "Question": "Summarize patient details of Ava Morales", "Expected Tool": "patient_claims + medtranscripts", "Status": "⚠️ Review"},
            {"Category": "Clinical Guidelines", "Question": "Evidence-based interventions to reduce readmissions?", "Expected Tool": "pubmed", "Status": "✅ Pass"},
        ])
        st.dataframe(eval_questions, hide_index=True, use_container_width=True)
    
    with st.container(border=True):
        st.subheader("Setup AI Observability")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Step 1: Install TruLens**")
            st.code("""pip install trulens-core \\
  trulens-connectors-snowflake \\
  trulens-providers-cortex""", language="bash")
            
            st.markdown("**Step 2: Configure Metrics**")
            st.code("""from trulens_providers_cortex import Cortex

provider = Cortex(conn, model="claude-3-5-sonnet")

# Define evaluation metrics
context_relevance = provider.context_relevance()
answer_relevance = provider.relevance()
groundedness = provider.groundedness()""", language="python")
        
        with col2:
            st.markdown("**Step 3: Run Evaluation**")
            st.code("""# Run against test dataset
for question in test_questions:
    response = agent.run(question)
    
    # Score with LLM-as-judge
    scores = {
        "context": context_relevance(question, context),
        "relevance": answer_relevance(question, response),
        "grounded": groundedness(response, context)
    }""", language="python")
            
            st.markdown("**Step 4: View in Snowsight**")
            st.info("**AI & ML → AI Observability** to compare evaluation runs")
    
    with st.container(border=True):
        st.subheader("User Feedback Summary")
        
        if demo_mode:
            col1, col2 = st.columns([1, 2])
            with col1:
                feedback_data = pd.DataFrame({
                    'Feedback': ['👍 Positive', '👎 Negative'],
                    'Count': [342, 23],
                    'Percentage': ['93.7%', '6.3%']
                })
                st.dataframe(feedback_data, hide_index=True, use_container_width=True)
            with col2:
                st.metric("Satisfaction Rate", "93.7%", delta="+2.1%", border=True)
        
        st.code(f"""-- Query user feedback
SELECT RECORD:feedback_type::STRING AS feedback, COUNT(*) AS count
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    '{AGENT_DATABASE}', '{AGENT_SCHEMA}', '{AGENT_NAME}', 'CORTEX AGENT'
))
WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK'
GROUP BY RECORD:feedback_type;""", language="sql")

st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.caption("Dashboard for Cortex Agent governance")
with col2:
    if demo_mode:
        st.caption("🔶 Demo Mode - Sample Data")
    else:
        st.caption("🟢 Live Data")
with col3:
    st.caption("Snowflake Intelligence")
