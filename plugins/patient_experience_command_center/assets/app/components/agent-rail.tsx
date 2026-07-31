"use client"
import React, { useEffect, useRef, useState } from "react"
import { Markdown } from "./markdown"

interface Cite { title: string; text: string }
interface Message { role: "user" | "agent"; text: string; cites?: Cite[]; streaming?: boolean; error?: boolean; status?: string }

/** Sample questions scoped per journey stage (answerable by the agent). */
const STAGE_QUESTIONS: Record<string, string[]> = {
  s1: [
    "How is arrival and access experience trending, and what is the ED wait?",
    "Which areas have the longest waits versus their target?",
    "What are patients saying about access and timeliness?",
  ],
  s2: [
    "What is the nurse communication top-box rate by patient race?",
    "Is communication a strength or a risk right now?",
    "What are patients saying about doctor and nurse communication?",
  ],
  s3: [
    "Which units have the lowest composite and how many high detractor-risk patients do they have?",
    "What is driving responsiveness down?",
    "What are patients saying about staff responsiveness and quietness?",
  ],
  s4: [
    "Why is care transition our biggest detractor?",
    "What are patients saying about discharge instructions and medications?",
    "Which measures contributed most to the composite drop this year?",
  ],
  s5: [
    "Which experience themes have the most negative sentiment?",
    "How many patients are awaiting service recovery?",
    "What are patients saying about follow-up and grievance handling?",
  ],
}

export function AgentRail({
  open,
  onClose,
  stageKey,
  stageName,
  seedQuestion,
  seedNonce,
}: {
  open: boolean
  onClose: () => void
  stageKey: string
  stageName: string
  seedQuestion?: string | null
  seedNonce?: number
}) {
  const [input, setInput] = useState("")
  const [messages, setMessages] = useState<Message[]>([])
  const [busy, setBusy] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const threadRef = useRef<HTMLDivElement>(null)
  const taRef = useRef<HTMLTextAreaElement>(null)
  const agentIdx = useRef(-1)
  const railRef = useRef<HTMLElement>(null)
  const [pos, setPos] = useState<{ left: number; top: number } | null>(null)
  const drag = useRef<{ dx: number; dy: number } | null>(null)

  const questions = STAGE_QUESTIONS[stageKey] ?? []

  const patch = (fn: (m: Message) => Message) =>
    setMessages((prev) => prev.map((m, i) => (i === agentIdx.current ? fn(m) : m)))

  async function ask(q: string) {
    const question = q.trim()
    if (!question || busy) return
    setMessages((prev) => {
      agentIdx.current = prev.length + 1
      return [...prev, { role: "user", text: question }, { role: "agent", text: "", streaming: true, status: "Thinking…", cites: [] }]
    })
    setInput("")
    if (taRef.current) taRef.current.style.height = "auto"
    setBusy(true)
    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, stage: stageName }),
      })
      if (!res.ok || !res.body) {
        const j = await res.json().catch(() => ({}))
        patch((m) => ({ ...m, error: true, streaming: false, status: "", text: `Sorry — the agent could not respond. ${j.error ?? res.status}${j.detail ? `\n${j.detail}` : ""}` }))
        setBusy(false); return
      }
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ""
      let gotText = false
      let gotCite = false
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const lines = buf.split("\n"); buf = lines.pop() ?? ""
        for (const line of lines) {
          if (!line.trim()) continue
          let evt: any
          try { evt = JSON.parse(line) } catch { continue }
          if (evt.t === "text") { gotText = true; patch((m) => ({ ...m, text: m.text + evt.v, status: "", streaming: true })) }
          else if (evt.t === "status") { patch((m) => (m.text ? m : { ...m, status: evt.v })) }
          else if (evt.t === "cite") { gotCite = true; patch((m) => ({ ...m, cites: [...(m.cites ?? []), { title: evt.title, text: evt.text }] })) }
          else if (evt.t === "error") { patch((m) => ({ ...m, error: true, streaming: false, status: "", text: m.text || `Agent error: ${evt.v}${evt.code ? ` (${evt.code})` : ""}` })) }
        }
      }
      patch((m) => ({ ...m, streaming: false, status: "", text: m.text || (gotText || gotCite ? m.text : "No answer returned.") }))
    } catch (e) {
      patch((m) => ({ ...m, error: true, streaming: false, status: "", text: `Request failed: ${e instanceof Error ? e.message : "unknown error"}` }))
    } finally {
      setBusy(false)
    }
  }

  function newChat() {
    setMessages([]); setInput("")
    if (taRef.current) { taRef.current.style.height = "auto"; taRef.current.focus() }
  }

  // Seed a question when an "Ask the agent →" link is clicked on the page.
  useEffect(() => {
    if (open && seedNonce && seedQuestion) ask(seedQuestion)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedNonce])

  // Fresh, scoped conversation whenever the journey stage changes.
  useEffect(() => { setMessages([]); setInput("") }, [stageKey])

  // Auto-scroll to the newest message.
  useEffect(() => { if (threadRef.current) threadRef.current.scrollTop = threadRef.current.scrollHeight }, [messages])

  // Focus the composer on open.
  useEffect(() => { if (open && taRef.current) taRef.current.focus() }, [open])

  function onKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); ask(input) }
  }
  function onChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    setInput(e.target.value)
    const el = e.target
    el.style.height = "auto"
    el.style.height = Math.min(el.scrollHeight, 120) + "px"
  }

  // Drag the window by its header (compact mode only).
  function onDragStart(e: React.PointerEvent<HTMLDivElement>) {
    if (expanded) return
    if ((e.target as HTMLElement).closest(".railctrls")) return
    const el = railRef.current
    if (!el) return
    const r = el.getBoundingClientRect()
    drag.current = { dx: e.clientX - r.left, dy: e.clientY - r.top }
    setPos({ left: r.left, top: r.top })
    e.currentTarget.setPointerCapture(e.pointerId)
    e.preventDefault()
  }
  function onDragMove(e: React.PointerEvent<HTMLDivElement>) {
    if (!drag.current) return
    const el = railRef.current
    if (!el) return
    const w = el.offsetWidth, h = el.offsetHeight
    const left = Math.max(8, Math.min(e.clientX - drag.current.dx, window.innerWidth - w - 8))
    const top = Math.max(8, Math.min(e.clientY - drag.current.dy, window.innerHeight - h - 8))
    setPos({ left, top })
  }
  function onDragEnd(e: React.PointerEvent<HTMLDivElement>) {
    drag.current = null
    try { e.currentTarget.releasePointerCapture(e.pointerId) } catch {}
  }

  return (
    <>
      {expanded && <div className="agentbackdrop open" onClick={() => setExpanded(false)} />}
      <aside
        ref={railRef}
        className={`rail${open ? " open" : ""}${expanded ? " expanded" : ""}`}
        aria-hidden={!open}
        style={pos && !expanded ? { left: pos.left, top: pos.top, right: "auto", bottom: "auto" } : undefined}
      >
        <div className="railtop" onPointerDown={onDragStart} onPointerMove={onDragMove} onPointerUp={onDragEnd} title="Drag to move">
          <div>
            <div className="rh"><span className="spark" />Experience Agent</div>
            <div className="scope">Scoped to: {stageName}</div>
          </div>
          <div className="railctrls">
            <button className="iconbtn" title="New chat" aria-label="New chat" onClick={newChat}>＋</button>
            <button className="iconbtn" title={expanded ? "Collapse" : "Expand to full screen"} aria-label="Expand" onClick={() => setExpanded((v) => !v)}>{expanded ? "⤡" : "⤢"}</button>
            <button className="iconbtn" title="Close" aria-label="Close" onClick={onClose}>×</button>
          </div>
        </div>

        <div className="thread" ref={threadRef}>
          {messages.length === 0 && (
            <div className="emptychat">
              <div className="ec-title">Ask about {stageName}</div>
              <div className="ec-sub">Grounded in the certified patient-experience data product — HCAHPS, Voice-of-Patient, and drivers.</div>
              <div className="chips">
                {questions.map((q, i) => (
                  <button key={i} className="chip" disabled={busy} onClick={() => ask(q)}>{q}</button>
                ))}
              </div>
            </div>
          )}
          {messages.map((m, i) =>
            m.role === "user" ? (
              <div className="turn" key={i}><div className="qbub">{m.text}</div></div>
            ) : (
              <div className="turn" key={i}>
                <div className={`abub${m.error ? " err" : ""}`}>
                  <div className="amrole"><span className="amdot" />Experience Agent</div>
                  {!m.text && m.streaming ? (
                    <div className="typing"><i /><i /><i /> {m.status || "Thinking…"}</div>
                  ) : (
                    <Markdown text={m.text} streaming={!!m.streaming} />
                  )}
                </div>
              </div>
            )
          )}
        </div>

        <div className="composer">
          <div className="cbar">
            <textarea
              ref={taRef}
              value={input}
              onChange={onChange}
              onKeyDown={onKeyDown}
              rows={1}
              placeholder={`Ask about ${stageName}…`}
              disabled={busy}
            />
            <button className="send" title="Send" aria-label="Send" disabled={busy || !input.trim()} onClick={() => ask(input)}>➤</button>
          </div>
          <div className="chint">Enter to send · Shift+Enter for newline</div>
        </div>
      </aside>
    </>
  )
}
