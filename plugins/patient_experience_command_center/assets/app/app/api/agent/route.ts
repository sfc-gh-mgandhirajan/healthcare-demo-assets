/**
 * Streaming proxy to the Patient Experience Cortex Agent (agent:run).
 * Reads the SPCS service token (or local dev OAuth) via lib/snowflake, calls the
 * named-agent run endpoint, parses the SSE stream, and re-emits newline-delimited
 * JSON events { t: "status"|"thinking"|"text"|"cite"|"error"|"done", ... } to the client.
 */
import { getRestApiAuthHeader, getSnowflakeBaseUrl } from "@/lib/snowflake"
import { PX_DATABASE, PX_SCHEMA, PX_AGENT } from "@/lib/constants"

export const dynamic = "force-dynamic"

const encoder = new TextEncoder()

export async function POST(req: Request) {
  let question = ""
  let stage = ""
  try {
    const body = await req.json()
    question = String(body.question ?? "").slice(0, 2000)
    stage = String(body.stage ?? "")
  } catch {
    return Response.json({ error: "Invalid request body" }, { status: 400 })
  }
  if (!question.trim()) return Response.json({ error: "Missing question" }, { status: 400 })

  const baseUrl = getSnowflakeBaseUrl()
  if (!baseUrl) return Response.json({ error: "No Snowflake base URL available" }, { status: 500 })

  const userText = stage ? `[Journey stage in focus: ${stage}] ${question}` : question
  const endpoint = `${baseUrl}/api/v2/databases/${PX_DATABASE}/schemas/${PX_SCHEMA}/agents/${PX_AGENT}:run`

  let upstream: Response
  try {
    upstream = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: getRestApiAuthHeader(),
        "X-Snowflake-Authorization-Token-Type": "OAUTH",
        "Content-Type": "application/json",
        Accept: "text/event-stream",
      },
      body: JSON.stringify({
        messages: [{ role: "user", content: [{ type: "text", text: userText }] }],
        stream: true,
      }),
    })
  } catch (e) {
    const msg = e instanceof Error ? e.message : "agent fetch failed"
    return Response.json({ error: msg }, { status: 502 })
  }

  if (!upstream.ok || !upstream.body) {
    const detail = await upstream.text().catch(() => "")
    console.error(new Date().toISOString(), "[agent] upstream error", upstream.status, detail.slice(0, 500))
    return Response.json({ error: `Agent error ${upstream.status}`, detail: detail.slice(0, 500) }, { status: 502 })
  }

  const stream = new ReadableStream({
    async start(controller) {
      const emit = (obj: Record<string, any>) => controller.enqueue(encoder.encode(JSON.stringify(obj) + "\n"))
      const reader = upstream.body!.getReader()
      const decoder = new TextDecoder()
      let buf = ""
      let currentEvent = ""
      try {
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buf += decoder.decode(value, { stream: true })
          const lines = buf.split("\n")
          buf = lines.pop() ?? ""
          for (const raw of lines) {
            const line = raw.replace(/\r$/, "")
            if (line.startsWith("event:")) { currentEvent = line.slice(6).trim(); continue }
            if (!line.startsWith("data:")) continue
            const data = line.slice(5).trim()
            if (!data) continue
            let obj: any
            try { obj = JSON.parse(data) } catch { continue }
            if (currentEvent === "response.text.delta" && typeof obj.text === "string") {
              emit({ t: "text", v: obj.text })
            } else if (currentEvent === "response.thinking.delta" && typeof obj.text === "string") {
              emit({ t: "thinking", v: obj.text })
            } else if (currentEvent === "response.status" && typeof obj.message === "string") {
              emit({ t: "status", v: obj.message })
            } else if (currentEvent === "response.text.annotation" && obj.annotation) {
              const a = obj.annotation
              emit({ t: "cite", title: a.doc_title ?? "Patient voice", text: a.text ?? "" })
            } else if (currentEvent === "error") {
              emit({ t: "error", v: obj.message ?? "Agent error", code: obj.code })
            }
          }
        }
        emit({ t: "done" })
      } catch (e) {
        emit({ t: "error", v: e instanceof Error ? e.message : "stream failed" })
      } finally {
        controller.close()
      }
    },
  })

  return new Response(stream, {
    headers: { "Content-Type": "application/x-ndjson; charset=utf-8", "Cache-Control": "no-store" },
  })
}
