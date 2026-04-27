import os
import json
import re
import time
import logging
import threading
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import snowflake.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = os.environ.get("STATIC_DIR", os.path.join(os.path.dirname(__file__), "..", "frontend", "dist"))
app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")
CORS(app)

AGENT_FQN = "UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.TMF_GOVERNANCE_AGENT"
DATABASE = "UNSTRUCTURED_HEALTHDATA"
SCHEMA = "EDWARDS_CLINICAL_DOCS"

_conn_cache = {"conn": None, "lock": threading.Lock(), "created": 0}


def _create_connection():
    token_path = "/snowflake/session/token"
    if os.path.exists(token_path):
        with open(token_path, "r") as f:
            token = f.read().strip()
        host = os.environ.get("SNOWFLAKE_HOST")
        account = os.environ.get("SNOWFLAKE_ACCOUNT")
        logger.info(f"SPCS OAuth connect: host={host}, account={account}")
        params = {
            "account": account,
            "token": token,
            "authenticator": "oauth",
            "database": DATABASE,
            "schema": SCHEMA,
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "BI_WH"),
        }
        if host:
            params["host"] = host
            params["protocol"] = "https"
            params["port"] = 443
        return snowflake.connector.connect(**params)
    else:
        conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME")
        if conn_name:
            return snowflake.connector.connect(
                connection_name=conn_name,
                database=DATABASE,
                schema=SCHEMA,
            )
        return snowflake.connector.connect(
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            user=os.environ.get("SNOWFLAKE_USER", ""),
            authenticator="externalbrowser",
            database=DATABASE,
            schema=SCHEMA,
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "BI_WH"),
        )


def get_snowflake_connection():
    with _conn_cache["lock"]:
        conn = _conn_cache["conn"]
        age = time.time() - _conn_cache["created"]
        if conn and age < 240:
            try:
                conn.cursor().execute("SELECT 1")
                return conn
            except Exception:
                logger.info("Cached connection stale, reconnecting")
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        conn = _create_connection()
        _conn_cache["conn"] = conn
        _conn_cache["created"] = time.time()
        return conn


def fix_markdown_tables(text):
    sep_pattern = re.compile(r'(\|[\s\-:]+)+\|')
    if not sep_pattern.search(text):
        return text
    text = re.sub(r'\|\s*\n?\s*\|', '|\n|', text)
    lines = text.split('\n')
    fixed = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('|') and line.endswith('|'):
            fixed.append(line)
        elif '|' in line and line.count('|') >= 2:
            parts = re.split(r'(\|(?:[^|]*\|)+)', line)
            for part in parts:
                part = part.strip()
                if part and part.startswith('|') and part.endswith('|'):
                    fixed.append(part)
                elif part:
                    if fixed and fixed[-1].startswith('|'):
                        fixed.append('')
                    fixed.append(part)
        else:
            if fixed and fixed[-1].startswith('|'):
                fixed.append('')
            fixed.append(line)
    return '\n'.join(fixed)


@app.route("/api/trials", methods=["GET"])
def get_trials():
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                trial_name,
                device_name,
                indication,
                nct_number,
                COUNT(*) AS doc_count,
                COUNT(CASE WHEN completeness_status = 'Current' THEN 1 END) AS current_count,
                COUNT(CASE WHEN completeness_status = 'Outdated' THEN 1 END) AS outdated_count,
                ROUND(100.0 * COUNT(CASE WHEN completeness_status = 'Current' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS completeness_pct,
                ROUND(AVG(days_since_upload), 0) AS avg_age
            FROM DOCUMENT_METADATA
            GROUP BY trial_name, device_name, indication, nct_number
            ORDER BY trial_name
        """)
        cols = [desc[0].lower() for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return jsonify(rows)
    except Exception as e:
        logger.error(f"Error fetching trials: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trials/<trial_name>/documents", methods=["GET"])
def get_trial_documents(trial_name):
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                file_path, trial_name, document_type, version, device_name,
                site_name, pi_name, amendment_number, tmf_zone, functional_group,
                completeness_status, days_since_upload
            FROM DOCUMENT_METADATA
            WHERE trial_name = %s
            ORDER BY document_type, version
        """, (trial_name,))
        cols = [desc[0].lower() for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return jsonify(rows)
    except Exception as e:
        logger.error(f"Error fetching documents: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/governance/summary", methods=["GET"])
def governance_summary():
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*) AS total_docs,
                COUNT(DISTINCT trial_name) AS total_trials,
                COUNT(CASE WHEN completeness_status = 'Current' THEN 1 END) AS current_docs,
                COUNT(CASE WHEN completeness_status = 'Outdated' THEN 1 END) AS outdated_docs,
                ROUND(100.0 * COUNT(CASE WHEN completeness_status = 'Current' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS overall_completeness,
                COUNT(DISTINCT site_name) AS total_sites
            FROM DOCUMENT_METADATA
        """)
        cols = [desc[0].lower() for desc in cur.description]
        row = dict(zip(cols, cur.fetchone()))
        return jsonify(row)
    except Exception as e:
        logger.error(f"Error fetching summary: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/governance/by-zone", methods=["GET"])
def governance_by_zone():
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                tmf_zone,
                COUNT(*) AS total,
                COUNT(CASE WHEN completeness_status = 'Current' THEN 1 END) AS current_count,
                COUNT(CASE WHEN completeness_status = 'Outdated' THEN 1 END) AS outdated_count,
                ROUND(100.0 * COUNT(CASE WHEN completeness_status = 'Current' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS completeness_pct
            FROM DOCUMENT_METADATA
            GROUP BY tmf_zone
            ORDER BY tmf_zone
        """)
        cols = [desc[0].lower() for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return jsonify(rows)
    except Exception as e:
        logger.error(f"Error fetching zones: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent/chat", methods=["POST"])
def agent_chat():
    data = request.json
    user_message = data.get("message", "")

    conn = get_snowflake_connection()
    try:
        request_body = json.dumps({
            "messages": [{"role": "user", "content": [{"type": "text", "text": user_message}]}]
        })

        sql = f"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN('{AGENT_FQN}', $${request_body}$$) AS resp"
        logger.info(f"DATA_AGENT_RUN call for: {user_message[:100]}")

        t0 = time.time()
        cur = conn.cursor()
        cur.execute(sql)
        raw = cur.fetchone()[0]
        elapsed = round(time.time() - t0, 1)

        resp = json.loads(raw) if isinstance(raw, str) else raw

        if "code" in resp and "message" in resp:
            logger.error(f"Agent error: {resp}")
            return jsonify({"error": resp.get("message", "Agent error")}), 500

        agent_text = ""
        tool_calls = []
        trace = {"steps": [], "elapsed_seconds": elapsed}

        for item in resp.get("content", []):
            itype = item.get("type")

            if itype == "thinking":
                thinking_text = item.get("thinking", {}).get("text", "")
                if thinking_text:
                    trace["steps"].append({"type": "thinking", "content": thinking_text})

            elif itype == "text":
                agent_text += item.get("text", "")

            elif itype == "tool_use":
                tu = item.get("tool_use", {})
                tool_name = tu.get("name", "")
                tool_type = tu.get("type", "")
                tool_input = tu.get("input", {})
                if tool_name and tool_name != "server_skill":
                    step = {
                        "type": "tool_use",
                        "tool": tool_name,
                        "tool_type": tool_type,
                        "query": tool_input.get("query", ""),
                    }
                    trace["steps"].append(step)

            elif itype == "tool_result":
                tr = item.get("tool_result", {})
                tool_name = tr.get("name", "")
                status = tr.get("status", "")
                if tool_name and tool_name != "server_skill":
                    tool_calls.append({"tool": tool_name, "status": status})
                    step = {"type": "tool_result", "tool": tool_name, "status": status}
                    for c in tr.get("content", []):
                        if c.get("type") == "json":
                            j = c.get("json", {})
                            if "sql" in j:
                                step["sql"] = j["sql"]
                            if "text" in j:
                                step["analyst_text"] = j["text"]
                            rs = j.get("result_set", {})
                            if rs:
                                meta = rs.get("resultSetMetaData", {})
                                cols = [col["name"] for col in meta.get("rowType", [])]
                                data_rows = rs.get("data", [])
                                step["columns"] = cols
                                step["preview_rows"] = data_rows[:5]
                                step["total_rows"] = meta.get("numRows", len(data_rows))
                        elif c.get("type") == "text":
                            step["search_results"] = c.get("text", "")[:500]
                    trace["steps"].append(step)

        clean_text = agent_text.strip()
        clean_text = re.sub(r'</?answer>', '', clean_text)
        clean_text = re.sub(r'<chart>[^<]*</chart>', '', clean_text)
        clean_text = clean_text.replace('\\|', '|')
        clean_text = clean_text.replace('\\n', '\n')
        clean_text = fix_markdown_tables(clean_text)
        clean_text = clean_text.strip()
        logger.info(f"Agent response (first 500 chars repr): {repr(clean_text[:500])}")

        return jsonify({
            "response": clean_text or "Agent returned no text response.",
            "tool_calls": tool_calls,
            "trace": trace,
            "metadata": resp.get("metadata", {}),
        })

    except Exception as e:
        logger.error(f"Agent chat error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/reports/audit/<trial_name>", methods=["GET"])
def generate_audit_report(trial_name):
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("CALL GENERATE_AUDIT_REPORT(%s)", (trial_name,))
        report = cur.fetchone()[0]
        return jsonify({"report": report, "trial_name": trial_name})
    except Exception as e:
        logger.error(f"Error generating audit report: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_react(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
