import os
import json
import httpx
from auth import load_private_key, generate_jwt_token


class CortexAgentExecutor:
    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT", "").replace("_", "-")
        self.account_locator = os.getenv("SNOWFLAKE_ACCOUNT_LOCATOR", "")
        self.user = os.getenv("SNOWFLAKE_USER", "")
        self.private_key_path = os.getenv("PRIVATE_KEY_PATH", "rsa_key.p8")
        self.agent_database = os.getenv("AGENT_DATABASE", "SNOWFLAKE_INTELLIGENCE")
        self.agent_schema = os.getenv("AGENT_SCHEMA", "AGENTS")
        self.agent_name = os.getenv("AGENT_NAME", "ChatWithPatientData")
        
        self.private_key = load_private_key(self.private_key_path)
        
    def _get_base_url(self) -> str:
        return f"https://{self.account}.snowflakecomputing.com"
    
    def _get_headers(self) -> dict:
        token = generate_jwt_token(self.account_locator, self.user, self.private_key)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
        }
    
    async def execute(self, user_text: str) -> str:
        if not user_text:
            return "No message content provided."
        
        base_url = self._get_base_url()
        
        tools = [
            {"tool_spec": {"name": "patient_med_transcripts", "type": "cortex_search"}},
            {"tool_spec": {"name": "pubmed-search", "type": "cortex_search"}},
            {"tool_spec": {"name": "Patient_Claims_Semantic_model", "type": "cortex_analyst_text_to_sql"}}
        ]
        
        tool_resources = {
            "Patient_Claims_Semantic_model": {
                "semantic_model_file": "@UNSTRUCTURED_HEALTHDATA.MED_TRANSCRIPTS.DEMO_STAGE/sample_semantic_model_hc_payer.yaml"
            },
            "patient_med_transcripts": {
                "id_column": "DOC_CHUNK",
                "name": "UNSTRUCTURED_HEALTHDATA.MED_TRANSCRIPTS.MED_TRANSCRIPTS_AUTOMATED"
            },
            "pubmed-search": {
                "id_column": "ARTICLE_URL",
                "name": "PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_CKE_SEARCH_SERVICE"
            }
        }
        
        response_instruction = """You are clinical analyst responsible for retrieving, extracting and summarizing critical patient information from two key data sources - structured patient and claims data and unstructured patient medical transcript data. You need to combine this data with any available references in pubmed research article database and provide correlation or references to any applicable citations in the published article."""

        api_url = f"{base_url}/api/v2/cortex/agent:run"
        
        payload = {
            "model": "claude-3-5-sonnet",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_text}]
                }
            ],
            "tools": tools,
            "tool_resources": tool_resources,
            "response_instruction": response_instruction
        }
        
        try:
            async with httpx.AsyncClient(timeout=180.0) as client:
                response = await client.post(
                    api_url,
                    headers=self._get_headers(),
                    json=payload,
                )
                
                if response.status_code != 200:
                    return f"Cortex API error: {response.status_code} - {response.text[:1000]}"
                
                text_response = ""
                tool_uses = []
                tool_results = []
                
                for line in response.text.split("\n"):
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                        if data_str and data_str != "[DONE]":
                            try:
                                data = json.loads(data_str)
                                delta = data.get("delta", {})
                                content = delta.get("content", [])
                                
                                if isinstance(content, list):
                                    for item in content:
                                        if isinstance(item, dict):
                                            item_type = item.get("type")
                                            if item_type == "text":
                                                text_response += item.get("text", "")
                                            elif item_type == "tool_use":
                                                tool_use = item.get("tool_use", {})
                                                tool_uses.append(tool_use)
                                            elif item_type == "tool_results":
                                                tool_result = item.get("tool_results", {})
                                                tool_results.append(tool_result)
                            except json.JSONDecodeError:
                                pass
                
                if text_response:
                    return text_response
                
                response_parts = []
                
                for i, (tool_use, tool_result) in enumerate(zip(tool_uses, tool_results)):
                    tool_name = tool_use.get("name", "Unknown")
                    response_parts.append(f"\n**Tool Used: {tool_name}**\n")
                    
                    result_content = tool_result.get("content", [])
                    for rc in result_content:
                        if rc.get("type") == "json":
                            json_data = rc.get("json", {})
                            if "sql" in json_data:
                                response_parts.append(f"Generated SQL:\n```sql\n{json_data['sql'][:500]}...\n```\n")
                            if "results" in json_data:
                                results = json_data["results"]
                                if isinstance(results, list) and len(results) > 0:
                                    response_parts.append(f"Results ({len(results)} rows):\n")
                                    for row in results[:5]:
                                        response_parts.append(f"  {json.dumps(row)}\n")
                                    if len(results) > 5:
                                        response_parts.append(f"  ... and {len(results) - 5} more rows\n")
                        elif rc.get("type") == "text":
                            response_parts.append(rc.get("text", ""))
                
                if response_parts:
                    return "".join(response_parts)
                
                return f"Response received but no text content found. Raw events: {response.text[:1500]}"
                
        except Exception as e:
            return f"Error calling Cortex Agent: {str(e)}"
