import fs from "fs"
import {
  getServiceToken,
  getSnowflakeBaseUrl,
  readTomlDefaultConnection,
} from "@/lib/snowflake"
import { DATA_DB, DATA_SCHEMA, AGENT_NAME } from "@/lib/constants"

export const dynamic = "force-dynamic"

interface AgentAuth {
  token: string
  tokenType: string
}

/**
 * Resolve the bearer token + token-type for the Cortex Agent Run REST API.
 *   - In SPCS: the service OAuth token (verified working per the data contract).
 *   - Local dev: the default connection's OAUTH token file, or a PAT.
 * Composed from exported snowflake helpers so lib/snowflake.ts stays untouched.
 */
function resolveAgentAuth(): AgentAuth | null {
  const svc = getServiceToken()
  if (svc) return { token: svc, tokenType: "OAUTH" }

  const conn = readTomlDefaultConnection()
  if (!conn) return null
  const authenticator = (typeof conn.authenticator === "string" ? conn.authenticator : "").toUpperCase()

  if (authenticator === "OAUTH" && conn.token_file_path) {
    try {
      return { token: fs.readFileSync(conn.token_file_path as string, "utf8").trim(), tokenType: "OAUTH" }
    } catch {
      /* fall through */
    }
  }
  if (typeof conn.token === "string" && conn.token) {
    const type = authenticator === "PROGRAMMATIC_ACCESS_TOKEN" ? "PROGRAMMATIC_ACCESS_TOKEN" : "OAUTH"
    return { token: conn.token, tokenType: type }
  }
  return null
}

/**
 * POST /api/agent
 * Body: { messages: [{ role, content: [{ type: "text", text }] }] }
 * Proxies to the Cortex Agent Run API and streams the SSE response through.
 */
export async function POST(req: Request) {
  let body: { messages?: unknown }
  try {
    body = await req.json()
  } catch {
    return Response.json({ error: "Invalid JSON body" }, { status: 400 })
  }
  if (!body?.messages || !Array.isArray(body.messages)) {
    return Response.json({ error: "Body must include a messages[] array" }, { status: 400 })
  }

  const baseUrl = getSnowflakeBaseUrl()
  if (!baseUrl) {
    return Response.json({ error: "Could not resolve Snowflake base URL" }, { status: 500 })
  }
  const auth = resolveAgentAuth()
  if (!auth) {
    return Response.json({ error: "No Snowflake credentials available for the agent" }, { status: 500 })
  }

  const url = `${baseUrl}/api/v2/databases/${DATA_DB}/schemas/${DATA_SCHEMA}/agents/${AGENT_NAME}:run`

  let upstream: Response
  try {
    upstream = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${auth.token}`,
        "X-Snowflake-Authorization-Token-Type": auth.tokenType,
        "Content-Type": "application/json",
        Accept: "text/event-stream",
      },
      body: JSON.stringify({ messages: body.messages }),
    })
  } catch (e) {
    console.error(new Date().toISOString(), "[api/agent] upstream fetch failed", e)
    return Response.json(
      { error: e instanceof Error ? e.message : "Agent request failed" },
      { status: 502 },
    )
  }

  if (!upstream.ok || !upstream.body) {
    const detail = await upstream.text().catch(() => "")
    console.error(new Date().toISOString(), `[api/agent] upstream ${upstream.status}`, detail.slice(0, 500))
    return Response.json(
      { error: `Agent API returned ${upstream.status}`, detail: detail.slice(0, 1000) },
      { status: upstream.status === 401 || upstream.status === 403 ? upstream.status : 502 },
    )
  }

  // Pass the SSE stream straight through to the browser.
  return new Response(upstream.body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  })
}
