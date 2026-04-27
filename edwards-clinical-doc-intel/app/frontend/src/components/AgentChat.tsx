import { useState, useRef, useEffect } from 'react'
import { Send, Bot, User, Loader2, Sparkles, Wrench, Stethoscope, FileCheck, Shield, TrendingUp } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import AgentTrace, { Trace } from './AgentTrace'

interface Message {
  role: 'user' | 'assistant'
  content: string
  tools?: { tool: string; content: string }[]
  trace?: Trace
}

const mdComponents = {
  table: ({ children, ...props }: React.HTMLAttributes<HTMLTableElement>) => (
    <div className="table-wrapper"><table {...props}>{children}</table></div>
  ),
}

const PERSONA_QUESTIONS = [
  {
    persona: 'CMO / VP Clinical Ops',
    icon: 'stethoscope',
    questions: [
      'Which trials have the lowest document completeness and what is the risk exposure?',
      'What are Medtronic, Abbott, and Boston Scientific doing in our therapeutic space? How do their trial designs compare to ours?',
    ],
  },
  {
    persona: 'TMF Manager',
    icon: 'filecheck',
    questions: [
      'Show me all protocol amendments across trials. Are any sites using outdated versions?',
      'What documents are missing from TRISCEND II and which TMF zones have gaps?',
    ],
  },
  {
    persona: 'VP Regulatory / Quality',
    icon: 'shield',
    questions: [
      'Generate an audit readiness report for the ALLIANCE trial',
      'What were the primary and secondary endpoints approved in our SAPIEN 3 regulatory submissions?',
    ],
  },
  {
    persona: 'Clinical Strategy',
    icon: 'trending',
    questions: [
      'What competing transcatheter tricuspid valve trials are currently recruiting and how do their endpoints compare to TRISCEND II?',
      'Compare enrollment criteria between our ENCIRCLE trial and similar mitral valve replacement trials on ClinicalTrials.gov',
    ],
  },
]

const PERSONA_ICONS: Record<string, React.ReactNode> = {
  stethoscope: <Stethoscope size={14} />,
  filecheck: <FileCheck size={14} />,
  shield: <Shield size={14} />,
  trending: <TrendingUp size={14} />,
}

const LOADING_PHASES = [
  'Routing to Cortex Agent...',
  'Selecting tools...',
  'Executing query...',
  'Synthesizing response...',
]

export default function AgentChat() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [loadingPhase, setLoadingPhase] = useState(0)
  const [threadId, setThreadId] = useState<string | null>(null)
  const [activeTrace, setActiveTrace] = useState<Trace | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)
  const phaseTimer = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loadingPhase])

  useEffect(() => {
    if (loading) {
      setLoadingPhase(0)
      let phase = 0
      phaseTimer.current = setInterval(() => {
        phase = Math.min(phase + 1, LOADING_PHASES.length - 1)
        setLoadingPhase(phase)
      }, 6000)
    } else {
      if (phaseTimer.current) clearInterval(phaseTimer.current)
      setLoadingPhase(0)
    }
    return () => { if (phaseTimer.current) clearInterval(phaseTimer.current) }
  }, [loading])

  const sendMessage = async (text: string) => {
    if (!text.trim() || loading) return
    const userMsg: Message = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)
    setActiveTrace(null)

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, thread_id: threadId }),
      })
      const data = await res.json()
      if (data.thread_id) setThreadId(data.thread_id)
      const trace = data.trace || null
      setActiveTrace(trace)
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: data.response || data.error || 'No response', tools: data.tool_calls, trace },
      ])
    } catch (err) {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Connection error. Please try again.' }])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex gap-4 h-[calc(100vh-140px)]">
      <div className="flex flex-col flex-1 min-w-0">
        <div className="flex items-center gap-3 mb-4">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-edwards-accent to-cyan-600 flex items-center justify-center">
            <Bot size={20} className="text-white" />
          </div>
          <div>
            <h2 className="text-lg font-bold text-edwards-dark">TMF Governance Assistant</h2>
            <p className="text-xs text-edwards-gray">Powered by Cortex Agent | eTMF Search + Analytics + Audit + ClinicalTrials.gov</p>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto scrollbar-thin bg-white rounded-xl border border-gray-200 shadow-sm p-4 space-y-4">
          {messages.length === 0 && (
            <div className="py-8">
              <div className="text-center mb-6">
                <Sparkles size={28} className="mx-auto text-edwards-accent mb-2 opacity-60" />
                <p className="text-sm text-edwards-gray">Ask me about trial documents, governance metrics, audit readiness, or competitive intelligence</p>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-2xl mx-auto">
                {PERSONA_QUESTIONS.map((group, gi) => (
                  <div key={gi} className="space-y-2">
                    <div className="flex items-center gap-2 px-1">
                      <span className="text-edwards-accent">{PERSONA_ICONS[group.icon]}</span>
                      <span className="text-xs font-semibold text-edwards-dark uppercase tracking-wide">{group.persona}</span>
                    </div>
                    {group.questions.map((q, qi) => (
                      <button
                        key={qi}
                        onClick={() => sendMessage(q)}
                        className="w-full text-left text-xs p-3 rounded-lg border border-gray-200 hover:border-edwards-accent hover:bg-cyan-50/30 transition-all text-edwards-slate leading-relaxed"
                      >
                        {q}
                      </button>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg, i) => (
            <div key={i} className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : ''}`}>
              {msg.role === 'assistant' && (
                <div className="w-7 h-7 rounded-lg bg-edwards-accent/10 flex items-center justify-center flex-shrink-0 mt-1">
                  <Bot size={14} className="text-edwards-accent" />
                </div>
              )}
              <div className={`max-w-[80%] ${msg.role === 'user' ? 'order-first' : ''}`}>
                {msg.role === 'assistant' && msg.tools && msg.tools.length > 0 && (
                  <div className="mb-2 flex flex-wrap gap-1.5">
                    {msg.tools.map((t, j) => (
                      <span key={j} className="inline-flex items-center gap-1 text-[10px] px-2 py-0.5 bg-edwards-accent/10 text-edwards-accent rounded-full font-medium">
                        <Wrench size={9} />
                        {t.tool}
                      </span>
                    ))}
                  </div>
                )}
                <div
                  className={`rounded-xl px-4 py-3 text-sm ${
                    msg.role === 'user'
                      ? 'bg-edwards-dark text-white'
                      : 'bg-gray-50 text-edwards-slate border border-gray-200'
                  } ${msg.role === 'assistant' && msg.trace ? 'cursor-pointer hover:border-edwards-accent/50 transition-colors' : ''}`}
                  onClick={() => { if (msg.role === 'assistant' && msg.trace) setActiveTrace(msg.trace) }}
                >
                  {msg.role === 'assistant' ? (
                    <div className="markdown-content">
                      <ReactMarkdown components={mdComponents}>{msg.content}</ReactMarkdown>
                    </div>
                  ) : msg.content}
                </div>
              </div>
              {msg.role === 'user' && (
                <div className="w-7 h-7 rounded-lg bg-edwards-dark flex items-center justify-center flex-shrink-0 mt-1">
                  <User size={14} className="text-white" />
                </div>
              )}
            </div>
          ))}

          {loading && (
            <div className="flex gap-3">
              <div className="w-7 h-7 rounded-lg bg-edwards-accent/10 flex items-center justify-center flex-shrink-0">
                <Loader2 size={14} className="text-edwards-accent animate-spin" />
              </div>
              <div className="bg-gray-50 rounded-xl px-4 py-3 border border-gray-200 min-w-[240px]">
                <div className="space-y-2">
                  {LOADING_PHASES.map((phase, i) => (
                    <div key={i} className={`flex items-center gap-2 text-xs transition-all duration-500 ${
                      i < loadingPhase ? 'text-green-600' : i === loadingPhase ? 'text-edwards-accent font-medium' : 'text-gray-300'
                    }`}>
                      {i < loadingPhase ? (
                        <span className="text-green-500">&#10003;</span>
                      ) : i === loadingPhase ? (
                        <Loader2 size={10} className="animate-spin" />
                      ) : (
                        <span className="w-2.5" />
                      )}
                      {phase}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
          <div ref={bottomRef} />
        </div>

        <div className="mt-3 flex gap-2">
          <input
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && sendMessage(input)}
            placeholder="Ask about trials, documents, competitive intelligence, or request an audit report..."
            className="flex-1 px-4 py-3 rounded-xl border border-gray-200 bg-white text-sm focus:outline-none focus:ring-2 focus:ring-edwards-accent/30 focus:border-edwards-accent"
            disabled={loading}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={loading || !input.trim()}
            className="px-4 py-3 bg-edwards-dark text-white rounded-xl hover:bg-edwards-slate transition-colors disabled:opacity-40"
          >
            <Send size={18} />
          </button>
        </div>
      </div>

      <div className="w-[340px] flex-shrink-0 bg-white rounded-xl border border-gray-200 shadow-sm p-4">
        <AgentTrace trace={activeTrace} />
      </div>
    </div>
  )
}
