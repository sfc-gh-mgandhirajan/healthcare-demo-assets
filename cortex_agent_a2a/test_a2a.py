#!/usr/bin/env python3
import argparse
import json
import requests
import uuid


def test_agent(server_url: str, query: str, card_only: bool = False):
    print(f"\n🔷 Snowflake Cortex A2A Agent Test Client")
    print(f"   Server: {server_url}")
    print("=" * 60)
    
    print("\n📋 Fetching Agent Card...")
    print("=" * 60)
    
    try:
        card_response = requests.get(f"{server_url}/.well-known/agent.json", timeout=10)
        if card_response.status_code == 200:
            card = card_response.json()
            print(f"Name: {card.get('name', 'N/A')}")
            print(f"Description: {card.get('description', 'N/A')[:100]}...")
            print(f"Version: {card.get('version', 'N/A')}")
            skills = card.get('skills', [])
            print(f"Skills: {[s.get('name') for s in skills]}")
            caps = card.get('capabilities', {})
            print(f"Streaming: {caps.get('streaming', False)}")
        else:
            print(f"❌ Failed to fetch agent card: {card_response.status_code}")
            return
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection Error: Could not connect to {server_url}")
        print("   Make sure the server is running with `python main.py`")
        return
    except Exception as e:
        print(f"❌ Error fetching agent card: {e}")
        return
    
    if card_only:
        print("\n✅ Card-only mode complete!")
        return
    
    print("=" * 60)
    print(f"📨 Sending Query: {query}")
    print("=" * 60)
    
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
        response = requests.post(
            server_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120
        )
        
        print(f"\n📥 Raw JSON Response:")
        result = response.json()
        print(json.dumps(result, indent=2)[:2000])
        
        print("\n" + "=" * 60)
        print("🤖 Agent Response")
        print("=" * 60)
        
        if "result" in result:
            res = result["result"]
            if "parts" in res:
                for part in res["parts"]:
                    if part.get("kind") == "text" or part.get("type") == "text":
                        print(part.get("text", ""))
            elif "message" in res and "parts" in res["message"]:
                for part in res["message"]["parts"]:
                    if part.get("kind") == "text" or part.get("type") == "text":
                        print(part.get("text", ""))
        elif "error" in result:
            print(f"❌ Error: {result['error']}")
        
        print("\n✅ Test completed successfully!")
        
    except requests.exceptions.Timeout:
        print("❌ Request timed out after 120 seconds")
    except Exception as e:
        print(f"❌ Error sending query: {e}")


def main():
    parser = argparse.ArgumentParser(description="Test the Cortex A2A Agent")
    parser.add_argument(
        "--query", 
        default="What patient data do you have access to?",
        help="The question to send to the agent"
    )
    parser.add_argument(
        "--url", 
        default="http://localhost:8000",
        help="Base URL of the A2A server"
    )
    parser.add_argument(
        "--card-only", 
        action="store_true",
        help="Only fetch the agent card, don't send a query"
    )
    
    args = parser.parse_args()
    test_agent(args.url, args.query, args.card_only)


if __name__ == "__main__":
    main()
