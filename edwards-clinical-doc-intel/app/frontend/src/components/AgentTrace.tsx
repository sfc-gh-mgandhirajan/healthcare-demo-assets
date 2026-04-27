import { Brain, Wrench, Database, Table2, Clock, CheckCircle2, Search, FileText, ChevronDown, ChevronRight, Info } from 'lucide-react'
import { useState } from 'react'

export interface TraceStep {
  type: 'thinking' | 'tool_use' | 'tool_result'
  content?: string
  tool?: string
  tool_type?: string
  query?: string
  status?: string
  sql?: string
  analyst_text?: string
  columns?: string[]
  preview_rows?: string[][]
  total_rows?: number
  search_results?: string
}

export interface Trace {
  steps: TraceStep[]
  elapsed_seconds: number
}

const TOOL_LABELS: Record<string, { label: string; description: string }> = {
  tmf_analytics: { label: 'Cortex Analyst', description: 'Text-to-SQL over governance metrics' },
  etmf_search: { label: 'eTMF Search', description: 'Semantic search across parsed clinical documents' },
  clinical_trials_registry: { label: 'ClinicalTrials.gov', description: 'Search 5.7M trial records from Marketplace CKE' },
  audit_report_gen: { label: 'Audit Report', description: 'Generate audit readiness report via stored procedure' },
}

function Collapsible({ title, icon, children, defaultOpen = true }: { title: string; icon: React.ReactNode; children: React.ReactNode; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="border border-gray-100 rounded-lg overflow-hidden">
      <button onClick={() => setOpen(!open)} className="w-full flex items-center gap-2 px-3 py-2 bg-gray-50/80 hover:bg-gray-100/80 transition-colors text-xs font-medium text-edwards-dark">
        {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        {icon}
        {title}
      </button>
      {open && <div className="px-3 py-2">{children}</div>}
    </div>
  )
}

export default function AgentTrace({ trace }: { trace: Trace | null }) {
  if (!trace) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-center px-4">
        <Info size={28} className="text-edwards-accent/40 mb-3" />
        <p className="text-sm font-medium text-edwards-dark mb-2">Agent Explainability</p>
        <p className="text-xs text-edwards-gray leading-relaxed">
          Ask a question and this panel will show the agent's reasoning chain — which tool it selected, what query it ran, and the raw results.
        </p>
        <div className="mt-4 space-y-2 text-left w-full">
          {Object.entries(TOOL_LABELS).map(([key, val]) => (
            <div key={key} className="flex items-start gap-2 text-xs">
              <Wrench size={10} className="text-edwards-accent mt-0.5 flex-shrink-0" />
              <div>
                <span className="font-medium text-edwards-dark">{val.label}</span>
                <span className="text-edwards-gray"> — {val.description}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  const thinkingSteps = trace.steps.filter(s => s.type === 'thinking')
  const toolUseSteps = trace.steps.filter(s => s.type === 'tool_use')
  const toolResultSteps = trace.steps.filter(s => s.type === 'tool_result')

  return (
    <div className="h-full overflow-y-auto scrollbar-thin space-y-3 pr-1">
      <div className="flex items-center justify-between mb-1">
        <p className="text-xs font-semibold text-edwards-dark uppercase tracking-wide">Agent Trace</p>
        <div className="flex items-center gap-1 text-xs text-edwards-gray">
          <Clock size={10} />
          {trace.elapsed_seconds}s
        </div>
      </div>

      {thinkingSteps.map((step, i) => (
        <Collapsible key={`think-${i}`} title="Reasoning" icon={<Brain size={12} className="text-purple-500" />}>
          <p className="text-xs text-edwards-slate leading-relaxed italic">{step.content}</p>
        </Collapsible>
      ))}

      {toolUseSteps.map((step, i) => {
        const info = TOOL_LABELS[step.tool || '']
        return (
          <Collapsible key={`use-${i}`} title={`Tool: ${info?.label || step.tool}`} icon={<Wrench size={12} className="text-blue-500" />}>
            <div className="space-y-1.5">
              {info && <p className="text-xs text-edwards-gray">{info.description}</p>}
              {step.tool_type && (
                <div className="flex items-center gap-1.5">
                  <span className="text-[10px] px-1.5 py-0.5 bg-blue-50 text-blue-600 rounded font-mono">{step.tool_type}</span>
                </div>
              )}
              {step.query && (
                <div>
                  <p className="text-[10px] text-edwards-gray uppercase tracking-wider mb-0.5">Query sent</p>
                  <p className="text-xs text-edwards-dark bg-gray-50 rounded px-2 py-1.5 leading-relaxed">{step.query}</p>
                </div>
              )}
            </div>
          </Collapsible>
        )
      })}

      {toolResultSteps.map((step, i) => {
        const info = TOOL_LABELS[step.tool || '']
        return (
          <Collapsible key={`result-${i}`} title={`Result: ${info?.label || step.tool}`} icon={
            step.status === 'success'
              ? <CheckCircle2 size={12} className="text-green-500" />
              : <Search size={12} className="text-orange-500" />
          }>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${step.status === 'success' ? 'bg-green-50 text-green-700' : 'bg-orange-50 text-orange-700'}`}>
                  {step.status}
                </span>
                {step.total_rows !== undefined && (
                  <span className="text-[10px] text-edwards-gray">{step.total_rows} rows returned</span>
                )}
              </div>

              {step.analyst_text && (
                <p className="text-xs text-edwards-slate italic">{step.analyst_text}</p>
              )}

              {step.sql && (
                <Collapsible title="SQL Generated" icon={<Database size={11} className="text-cyan-600" />} defaultOpen={false}>
                  <pre className="text-[10px] text-edwards-slate bg-gray-900 text-green-300 rounded-md px-3 py-2 overflow-x-auto whitespace-pre-wrap font-mono leading-relaxed">{step.sql.replace(/ -- Generated by Cortex Analyst.*$/, '').trim()}</pre>
                </Collapsible>
              )}

              {step.columns && step.preview_rows && step.preview_rows.length > 0 && (
                <Collapsible title="Result Preview" icon={<Table2 size={11} className="text-emerald-600" />} defaultOpen={false}>
                  <div className="overflow-x-auto">
                    <table className="text-[10px] w-full">
                      <thead>
                        <tr className="border-b border-gray-200">
                          {step.columns.map((col, ci) => (
                            <th key={ci} className="text-left px-1.5 py-1 font-semibold text-edwards-dark whitespace-nowrap">{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {step.preview_rows.map((row, ri) => (
                          <tr key={ri} className="border-b border-gray-50">
                            {row.map((cell, ci) => (
                              <td key={ci} className="px-1.5 py-1 text-edwards-slate whitespace-nowrap">{cell}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </Collapsible>
              )}

              {step.search_results && (
                <Collapsible title="Search Results" icon={<FileText size={11} className="text-amber-600" />} defaultOpen={false}>
                  <p className="text-[10px] text-edwards-slate leading-relaxed whitespace-pre-wrap">{step.search_results}</p>
                </Collapsible>
              )}
            </div>
          </Collapsible>
        )
      })}
    </div>
  )
}
