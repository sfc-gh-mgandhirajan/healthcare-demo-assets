import streamlit as st
import requests
import json
import uuid
import os
from pathlib import Path

st.set_page_config(
    page_title="Patient Data Agent - A2A Client",
    page_icon="🏥",
    layout="wide"
)

def get_spcs_auth_token():
    token_file = Path.home() / ".snowflake" / "tokens" / "Murali-AWS-US_WEST_token"
    if token_file.exists():
        with open(token_file, "r") as f:
            return f.read().strip()
    return None

st.title("🏥 Patient Data Agent")
st.markdown("**A2A Protocol Client for ChatWithPatientData Agent**")

with st.sidebar:
    st.header("⚙️ Configuration")
    
    service_mode = st.radio(
        "Service Mode",
        ["Local (localhost:8000)", "SPCS (Snowflake)"],
        index=0
    )
    
    if service_mode == "Local (localhost:8000)":
        a2a_url = "http://localhost:8000"
        auth_headers = {}
    else:
        a2a_url = st.text_input(
            "SPCS Service URL",
            value="https://j7b4ahti-sfsenorthamerica-polaris1.snowflakecomputing.app",
            help="Enter the SPCS ingress URL"
        )
        token = get_spcs_auth_token()
        if token:
            auth_headers = {"Authorization": f"Snowflake Token=\"{token}\""}
            st.success("✓ Auth token loaded")
        else:
            st.warning("⚠️ No auth token found")
            auth_headers = {}
    
    st.divider()
    
    if st.button("🔍 Fetch Agent Card"):
        try:
            headers = {"Content-Type": "application/json"}
            headers.update(auth_headers)
            response = requests.get(f"{a2a_url}/.well-known/agent.json", headers=headers, timeout=30)
            if response.status_code == 200:
                card = response.json()
                st.success("Agent discovered!")
                st.json(card)
            else:
                st.error(f"Error: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            st.error(f"Connection failed: {e}")

st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("💬 Query the Agent")
    
    sample_queries = [
        "List patients with diabetes from claims data",
        "Search for patients with cardiac conditions in medical transcripts",
        "Find pubmed articles related to hypertension treatment",
        "Show me patient claims summary by diagnosis",
        "What patient data sources are available?"
    ]
    
    selected_sample = st.selectbox(
        "Sample queries:",
        ["-- Select a sample query --"] + sample_queries
    )
    
    if selected_sample != "-- Select a sample query --":
        query = st.text_area("Your query:", value=selected_sample, height=100)
    else:
        query = st.text_area("Your query:", placeholder="Enter your question about patient data...", height=100)

with col2:
    st.subheader("📊 Agent Info")
    st.info(f"""
    **Agent:** ChatWithPatientData
    
    **Tools:**
    - 📄 Patient Medical Transcripts
    - 🔬 PubMed Research Articles  
    - 📋 Patient Claims (Semantic Model)
    
    **Protocol:** A2A (JSON-RPC 2.0)
    """)

st.markdown("---")

if st.button("🚀 Send Query", type="primary", use_container_width=True):
    if not query:
        st.warning("Please enter a query")
    elif not a2a_url:
        st.warning("Please configure the A2A service URL")
    else:
        with st.spinner("Querying agent via A2A protocol..."):
            request_id = str(uuid.uuid4())
            message_id = str(uuid.uuid4())
            
            payload = {
                "jsonrpc": "2.0",
                "method": "message/send",
                "id": request_id,
                "params": {
                    "message": {
                        "messageId": message_id,
                        "role": "user",
                        "parts": [{"type": "text", "text": query}]
                    }
                }
            }
            
            try:
                headers = {"Content-Type": "application/json"}
                if service_mode == "SPCS (Snowflake)":
                    token = get_spcs_auth_token()
                    if token:
                        headers["Authorization"] = f"Snowflake Token=\"{token}\""
                
                response = requests.post(
                    a2a_url,
                    json=payload,
                    headers=headers,
                    timeout=180
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    st.success("✅ Response received!")
                    
                    if "result" in result:
                        res = result["result"]
                        parts = res.get("parts", [])
                        
                        for part in parts:
                            text = part.get("text", "")
                            if text:
                                if "```sql" in text:
                                    st.subheader("🤖 Agent Response")
                                    st.markdown(text)
                                elif "**Tool Used:" in text:
                                    st.subheader("🔧 Tool Execution")
                                    st.markdown(text)
                                else:
                                    st.subheader("🤖 Agent Response")
                                    st.write(text)
                    
                    with st.expander("📜 Raw JSON Response"):
                        st.json(result)
                        
                elif response.status_code == 401:
                    st.error("Authentication failed. Check token configuration.")
                else:
                    st.error(f"Error: {response.status_code} - {response.text[:500]}")
                    
            except requests.exceptions.Timeout:
                st.error("Request timed out after 180 seconds")
            except requests.exceptions.ConnectionError:
                st.error(f"Could not connect to {a2a_url}. Is the service running?")
            except Exception as e:
                st.error(f"Error: {str(e)}")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

st.markdown("---")
st.caption("Built with Streamlit | A2A Protocol Client for Snowflake Cortex Agents")
