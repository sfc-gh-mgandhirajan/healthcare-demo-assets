"use client"

import { useEffect, useRef, useState } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

interface ChatMsg {
  role: "user" | "assistant"
  text: string
}

const SUGGESTED = [
  "How is net pharmacy PMPM trending year over year?",
  "Which therapeutic classes are driving the cost increase?",
  "How much of spend is specialty versus traditional?",
  "What do our coverage policies say about GLP-1 appropriateness?",
]

export function AgentDrawer({ open, onOpen, onClose }: { open: boolean; onOpen: () => void; onClose: () => void }) {
  const [messages, setMessages] = useState<ChatMsg[]>([])
  const [input, setInput] = useState("")
  const [busy, setBusy] = useState(false)
  const [status, setStatus] = useState<string | null>(null)
  const [note, setNote] = useState<string | null>(null)
  const convoRef = useRef<HTMLDivElement>(null)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    convoRef.current?.scrollTo({ top: convoRef.current.scrollHeight, behavior: "smooth" })
  }, [messages, busy, status])

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape" && open) onClose()
    }
    document.addEventListener("keydown", onKey)
    return () => document.removeEventListener("keydown", onKey)
  }, [open, onClose])

  async function send(text: string) {
    const q = text.trim()
    if (!q || busy) return
    setInput("")
    setNote(null)
    setStatus("Planning…")
    const history = [...messages, { role: "user" as const, text: q }]
    setMessages([...history, { role: "assistant", text: "" }])
    setBusy(true)
    const controller = new AbortController()
    abortRef.current = controller

    const payload = {
      messages: history.map((m) => ({ role: m.role, content: [{ type: "text", text: m.text }] })),
    }

    let assembled = ""
    const pushAssembled = () =>
      setMessages((prev) => {
        const next = [...prev]
        next[next.length - 1] = { role: "assistant", text: assembled }
        return next
      })

    // Only render the final answer text; ignore the model's chain-of-thought.
    const applyEvent = (block: string) => {
      const lines = block.split("\n")
      const evLine = lines.find((l) => l.startsWith("event:"))
      const eventName = evLine ? evLine.slice(6).trim() : ""
      const dataRaw = lines
        .filter((l) => l.startsWith("data:"))
        .map((l) => l.slice(5).trim())
        .join("\n")
      if (!dataRaw || dataRaw === "[DONE]") return
      let obj: any
      try {
        obj = JSON.parse(dataRaw)
      } catch {
        return
      }

      switch (eventName) {
        case "response.status":
          if (obj?.message) setStatus(String(obj.message))
          return
        case "response.text.delta":
          if (typeof obj?.text === "string") {
            assembled += obj.text
            setStatus(null)
            pushAssembled()
          }
          return
        case "response.text":
          if (!assembled && typeof obj?.text === "string") {
            assembled = obj.text
            setStatus(null)
            pushAssembled()
          }
          return
        default:
          // thinking / tool_use / tool_result / suggested_queries / response / done — ignore
          return
      }
    }

    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal,
      })
      if (!res.ok || !res.body) {
        const body = await res.json().catch(() => ({}))
        throw new Error(body?.detail || body?.error || `Agent error (${res.status})`)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        let idx
        while ((idx = buffer.indexOf("\n\n")) !== -1) {
          const block = buffer.slice(0, idx)
          buffer = buffer.slice(idx + 2)
          if (block.trim()) applyEvent(block)
        }
      }
      if (buffer.trim()) applyEvent(buffer)

      if (!assembled) {
        assembled = "_(No answer text was returned. Try rephrasing your question.)_"
        pushAssembled()
      }
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") return // superseded by New chat
      const msg = e instanceof Error ? e.message : "Agent request failed"
      assembled = `⚠️ ${msg}`
      pushAssembled()
      setNote(msg)
    } finally {
      if (abortRef.current === controller) abortRef.current = null
      setStatus(null)
      setBusy(false)
    }
  }

  /** Reset the drawer to a fresh conversation, cancelling any in-flight stream. */
  function newChat() {
    abortRef.current?.abort()
    abortRef.current = null
    setBusy(false)
    setStatus(null)
    setNote(null)
    setInput("")
    setMessages([])
  }

  return (
    <>
      <button className={"agentfab" + (open ? " hidden" : "")} type="button" onClick={onOpen}>
        <span className="spark" />
        Ask the agent
      </button>
      <div className={"agentbackdrop" + (open ? " open" : "")} onClick={onClose} />
      <aside className={"rail" + (open ? " open" : "")} aria-hidden={!open}>
        <div className="railtop">
          <div>
            <div className="rh">
              <span className="spark" />
              Cost Intelligence Agent
            </div>
            <div className="scope">Live · Cortex Agent over the certified pharmacy-cost book</div>
          </div>
          <div className="railactions">
            {messages.length > 0 ? (
              <button className="newchat" type="button" onClick={newChat} title="Start a new chat" aria-label="New chat">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 20h9" />
                  <path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4Z" />
                </svg>
              </button>
            ) : null}
            <button className="agentclose" type="button" aria-label="Close" onClick={onClose}>
              ×
            </button>
          </div>
        </div>

        {messages.length === 0 ? (
          <div className="chips">
            {SUGGESTED.map((q) => (
              <button key={q} className="chip" onClick={() => send(q)}>
                {q}
              </button>
            ))}
          </div>
        ) : null}

        <div className="convo" ref={convoRef}>
          {messages.map((m, i) => (
            <div key={i} className={"msg " + m.role}>
              {m.role === "assistant" ? (
                m.text ? (
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{m.text}</ReactMarkdown>
                ) : (
                  <span className="typing">{status ?? "thinking…"}</span>
                )
              ) : (
                m.text
              )}
            </div>
          ))}
        </div>

        <form
          className="composer"
          onSubmit={(e) => {
            e.preventDefault()
            send(input)
          }}
        >
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about spend, drivers, specialty, or policies…"
            disabled={busy}
          />
          <button className="btn" type="submit" disabled={busy || !input.trim()}>
            {busy ? "…" : "Send"}
          </button>
        </form>
        {note ? <div className="typing" style={{ marginTop: 6, color: "var(--bad)" }}>{note}</div> : null}
      </aside>
    </>
  )
}
