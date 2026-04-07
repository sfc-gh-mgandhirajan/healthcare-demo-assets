import os
import uuid
from pathlib import Path
from dotenv import load_dotenv

script_dir = Path(__file__).parent.resolve()
load_dotenv(script_dir / ".env")

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

USE_SPCS = os.getenv("USE_SPCS", "false").lower() == "true"

if USE_SPCS:
    from executor_spcs import CortexAgentExecutorSPCS as CortexAgentExecutor
else:
    from executor import CortexAgentExecutor

app = FastAPI()
executor = CortexAgentExecutor()


def get_agent_card():
    agent_name = os.getenv("AGENT_NAME", "ChatWithPatientData")
    agent_description = os.getenv(
        "AGENT_DESCRIPTION",
        "Snowflake Intelligence agent for patient data - correlates patient claims, medical transcripts with PubMed research articles."
    )
    agent_url = os.getenv("AGENT_URL", "http://localhost:8000")
    
    return {
        "name": f"Cortex Agent: {agent_name}",
        "description": agent_description,
        "url": agent_url,
        "version": "1.0.0",
        "skills": [
            {
                "id": "query_patient_data",
                "name": "Patient Data Query",
                "description": "Query patient claims, medical transcripts, and correlate with PubMed research articles.",
                "tags": ["snowflake", "healthcare", "patient", "claims", "pubmed", "cortex"],
            }
        ],
        "capabilities": {"streaming": False, "pushNotifications": False},
    }


@app.get("/")
async def health_check():
    return {"status": "healthy", "service": "A2A Cortex Agent Wrapper"}


@app.get("/.well-known/agent.json")
async def agent_card():
    return JSONResponse(content=get_agent_card())


@app.post("/")
async def handle_request(request: Request):
    body = await request.json()
    
    if body.get("jsonrpc") != "2.0":
        return JSONResponse(
            content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": None}
        )
    
    method = body.get("method")
    request_id = body.get("id")
    params = body.get("params", {})
    
    if method == "message/send":
        message = params.get("message", {})
        parts = message.get("parts", [])
        
        user_text = ""
        for part in parts:
            if part.get("type") == "text":
                user_text += part.get("text", "")
        
        agent_response = await executor.execute(user_text)
        
        return JSONResponse(content={
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "kind": "message",
                "messageId": str(uuid.uuid4()),
                "role": "agent",
                "parts": [{"kind": "text", "text": agent_response}]
            }
        })
    
    return JSONResponse(
        content={"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": request_id}
    )


def main():
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    host = os.getenv("HOSTNAME", "0.0.0.0")
    
    print(f"\n🔷 Snowflake Cortex A2A Agent Server")
    print(f"   Agent: {os.getenv('AGENT_NAME', 'ChatWithPatientData')}")
    print(f"   SPCS Mode: {USE_SPCS}")
    print(f"   Running on: http://{host}:{port}")
    print(f"   Discovery: http://{host}:{port}/.well-known/agent.json\n")
    
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
