import { useState, useEffect } from 'react'
import { Activity, FileText, AlertTriangle, CheckCircle, TrendingDown, ShieldAlert, ArrowRight } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

interface Trial {
  trial_name: string
  device_name: string
  indication: string
  nct_number: string
  doc_count: number
  current_count: number
  outdated_count: number
  completeness_pct: number
  avg_age: number
}

interface Summary {
  total_docs: number
  total_trials: number
  current_docs: number
  outdated_docs: number
  overall_completeness: number
  total_sites: number
}

interface Props {
  onTrialSelect: (trial: Trial) => void
}

function getRAG(pct: number): { color: string; bg: string; label: string; border: string } {
  if (pct >= 90) return { color: 'text-emerald-700', bg: 'bg-emerald-50', label: 'GREEN', border: 'border-emerald-200' }
  if (pct >= 70) return { color: 'text-amber-700', bg: 'bg-amber-50', label: 'AMBER', border: 'border-amber-200' }
  return { color: 'text-red-700', bg: 'bg-red-50', label: 'RED', border: 'border-red-200' }
}

function buildNarrative(trials: Trial[], summary: Summary): string[] {
  const lines: string[] = []

  const sorted = [...trials].sort((a, b) => a.completeness_pct - b.completeness_pct)
  const atRisk = sorted.filter(t => t.completeness_pct < 80)
  const healthy = sorted.filter(t => t.completeness_pct >= 90)

  if (atRisk.length === 0) {
    lines.push(`All ${summary.total_trials} active trials are above 80% document completeness. Portfolio is in strong governance posture.`)
  } else {
    lines.push(`${atRisk.length} of ${summary.total_trials} trials are below 80% completeness — ${atRisk.map(t => `${t.trial_name} (${t.completeness_pct}%)`).join(', ')}. These require immediate TMF remediation.`)
  }

  if (summary.outdated_docs > 0) {
    const pct = Math.round((summary.outdated_docs / summary.total_docs) * 100)
    lines.push(`${summary.outdated_docs} of ${summary.total_docs} documents (${pct}%) are flagged as outdated. Prioritize version updates before any inspection milestone.`)
  }

  const lowestTrial = sorted[0]
  if (lowestTrial && lowestTrial.completeness_pct < 90) {
    lines.push(`Highest risk: ${lowestTrial.trial_name} at ${lowestTrial.completeness_pct}% completeness with ${lowestTrial.outdated_count} outdated document${lowestTrial.outdated_count !== 1 ? 's' : ''}.`)
  }

  if (healthy.length > 0) {
    lines.push(`${healthy.map(t => t.trial_name).join(', ')} ${healthy.length === 1 ? 'is' : 'are'} audit-ready at ${healthy.length === 1 ? healthy[0].completeness_pct + '%' : '90%+'} completeness.`)
  }

  return lines
}

export default function TrialPortfolio({ onTrialSelect }: Props) {
  const [trials, setTrials] = useState<Trial[]>([])
  const [summary, setSummary] = useState<Summary | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/api/trials').then(r => r.json()),
      fetch('/api/governance/summary').then(r => r.json()),
    ]).then(([t, s]) => {
      setTrials(t)
      setSummary(s)
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="space-y-4">
        {[1, 2, 3].map(i => (
          <div key={i} className="h-32 bg-white rounded-xl animate-pulse" />
        ))}
      </div>
    )
  }

  const chartData = trials.map(t => ({
    name: t.trial_name,
    completeness: t.completeness_pct,
    docs: t.doc_count,
  }))

  const barColors = chartData.map(d => {
    if (d.completeness >= 90) return '#059669'
    if (d.completeness >= 70) return '#D97706'
    return '#DC2626'
  })

  const narrative = summary ? buildNarrative(trials, summary) : []

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-2xl font-bold text-edwards-dark">Clinical Trial Portfolio</h1>
          <p className="text-sm text-edwards-gray mt-1">Real-time TMF governance across all active Edwards Lifesciences trials</p>
        </div>
        {summary && (
          <div className="flex items-center gap-6">
            <Stat icon={Activity} label="Trials" value={summary.total_trials} />
            <Stat icon={FileText} label="Documents" value={summary.total_docs} />
            <Stat icon={CheckCircle} label="Completeness" value={`${summary.overall_completeness}%`} highlight={getRAG(summary.overall_completeness).color} />
            <Stat icon={AlertTriangle} label="Outdated" value={summary.outdated_docs} highlight={summary.outdated_docs > 0 ? 'text-amber-600' : undefined} />
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <div className="lg:col-span-3 bg-white rounded-xl border border-gray-200 shadow-sm p-6">
          <h2 className="text-sm font-semibold text-edwards-slate mb-4 uppercase tracking-wider">Completeness by Trial</h2>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chartData} layout="vertical" margin={{ left: 80 }}>
              <XAxis type="number" domain={[0, 100]} tick={{ fontSize: 12 }} tickFormatter={(v: number) => `${v}%`} />
              <YAxis type="category" dataKey="name" tick={{ fontSize: 12 }} width={80} />
              <Tooltip formatter={(v: number) => `${v}%`} />
              <Bar dataKey="completeness" radius={[0, 4, 4, 0]} barSize={24}>
                {chartData.map((_, i) => (
                  <Cell key={i} fill={barColors[i]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="lg:col-span-2 bg-white rounded-xl border border-gray-200 shadow-sm p-6 flex flex-col">
          <div className="flex items-center gap-2 mb-4">
            <ShieldAlert size={16} className="text-edwards-accent" />
            <h2 className="text-sm font-semibold text-edwards-slate uppercase tracking-wider">Portfolio Assessment</h2>
          </div>
          <div className="flex-1 space-y-3">
            {narrative.map((line, i) => (
              <div key={i} className="flex gap-2.5">
                <TrendingDown size={14} className={`flex-shrink-0 mt-0.5 ${i === 0 ? 'text-edwards-accent' : 'text-edwards-gray'}`} />
                <p className="text-sm text-edwards-slate leading-relaxed">{line}</p>
              </div>
            ))}
          </div>
          <div className="mt-4 pt-3 border-t border-gray-100">
            <p className="text-[10px] text-edwards-gray uppercase tracking-wider">Derived from TMF governance data in real-time</p>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-100">
          <h2 className="text-sm font-semibold text-edwards-slate uppercase tracking-wider">Trial Details</h2>
        </div>
        <table className="w-full">
          <thead>
            <tr className="bg-gray-50 text-xs text-edwards-gray uppercase tracking-wider">
              <th className="px-6 py-3 text-left">Trial</th>
              <th className="px-6 py-3 text-left">Device</th>
              <th className="px-6 py-3 text-left">Indication</th>
              <th className="px-6 py-3 text-center">Docs</th>
              <th className="px-6 py-3 text-center">Completeness</th>
              <th className="px-6 py-3 text-center">Avg Age</th>
              <th className="px-6 py-3 text-center">Status</th>
              <th className="px-6 py-3 text-center"></th>
            </tr>
          </thead>
          <tbody>
            {trials.map(t => {
              const rag = getRAG(t.completeness_pct)
              return (
                <tr
                  key={t.trial_name}
                  onClick={() => onTrialSelect(t)}
                  className="border-t border-gray-100 hover:bg-edwards-light/50 cursor-pointer transition-colors group"
                >
                  <td className="px-6 py-4">
                    <p className="text-sm font-semibold text-edwards-dark">{t.trial_name}</p>
                    <p className="text-xs text-edwards-gray">{t.nct_number}</p>
                  </td>
                  <td className="px-6 py-4 text-sm text-edwards-slate">{t.device_name}</td>
                  <td className="px-6 py-4 text-sm text-edwards-slate max-w-[200px] truncate">{t.indication}</td>
                  <td className="px-6 py-4 text-center">
                    <span className="text-sm font-medium">{t.doc_count}</span>
                    {t.outdated_count > 0 && (
                      <span className="ml-1 text-xs text-amber-600">({t.outdated_count} outdated)</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-center">
                    <span className={`text-sm font-bold ${rag.color}`}>{t.completeness_pct}%</span>
                  </td>
                  <td className="px-6 py-4 text-center text-sm text-edwards-gray">{t.avg_age}d</td>
                  <td className="px-6 py-4 text-center">
                    <span className={`text-xs font-semibold px-2 py-1 rounded border ${rag.bg} ${rag.border} ${rag.color}`}>
                      {rag.label}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-center">
                    <ArrowRight size={14} className="text-gray-300 group-hover:text-edwards-accent transition-colors" />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function Stat({ icon: Icon, label, value, highlight }: { icon: any; label: string; value: string | number; highlight?: string }) {
  return (
    <div className="text-right">
      <div className="flex items-center gap-1.5 justify-end mb-0.5">
        <Icon size={13} className="text-edwards-gray" />
        <span className="text-[10px] text-edwards-gray uppercase tracking-wider font-medium">{label}</span>
      </div>
      <p className={`text-xl font-bold ${highlight || 'text-edwards-dark'}`}>{value}</p>
    </div>
  )
}
