import { useState, useEffect } from 'react'
import { ArrowLeft, FileText, CheckCircle, AlertTriangle, Clock, MapPin } from 'lucide-react'

interface Trial {
  trial_name: string
  device_name?: string
  indication?: string
}

interface Document {
  file_path: string
  trial_name: string
  document_type: string
  version: string
  device_name: string
  site_name: string
  pi_name: string
  amendment_number: string
  tmf_zone: string
  functional_group: string
  completeness_status: string
  days_since_upload: number
}

interface Props {
  trial: Trial | null
  onAuditRequest: (trialName: string) => void
  onBack: () => void
}

export default function TrialDrilldown({ trial, onAuditRequest, onBack }: Props) {
  const [documents, setDocuments] = useState<Document[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!trial) return
    fetch(`/api/trials/${encodeURIComponent(trial.trial_name)}/documents`)
      .then(r => r.json())
      .then(docs => {
        setDocuments(docs)
        setLoading(false)
      })
  }, [trial])

  if (!trial) {
    return (
      <div className="text-center py-20 text-edwards-gray">
        <p>Select a trial from the portfolio to view details</p>
        <button onClick={onBack} className="mt-4 text-sm text-edwards-accent hover:underline">Go to Portfolio</button>
      </div>
    )
  }

  const current = documents.filter(d => d.completeness_status === 'Current')
  const outdated = documents.filter(d => d.completeness_status === 'Outdated')
  const byType: Record<string, Document[]> = {}
  documents.forEach(d => {
    if (!byType[d.document_type]) byType[d.document_type] = []
    byType[d.document_type].push(d)
  })

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <button onClick={onBack} className="p-2 rounded-lg hover:bg-white transition-colors">
          <ArrowLeft size={20} className="text-edwards-gray" />
        </button>
        <div>
          <h1 className="text-xl font-bold text-edwards-dark">{trial.trial_name} Trial</h1>
          {trial.device_name && <p className="text-sm text-edwards-gray">{trial.device_name} | {trial.indication}</p>}
        </div>
        <div className="ml-auto">
          <button
            onClick={() => onAuditRequest(trial.trial_name)}
            className="px-4 py-2 bg-edwards-red text-white text-sm font-medium rounded-lg hover:bg-red-700 transition-colors flex items-center gap-2"
          >
            <FileText size={16} />
            Generate Audit Report
          </button>
        </div>
      </div>

      <div className="grid grid-cols-4 gap-4">
        <StatCard icon={FileText} label="Total Documents" value={documents.length} />
        <StatCard icon={CheckCircle} label="Current" value={current.length} color="text-emerald-600" />
        <StatCard icon={AlertTriangle} label="Outdated" value={outdated.length} color={outdated.length > 0 ? 'text-amber-600' : 'text-emerald-600'} />
        <StatCard icon={Clock} label="Avg Age (days)" value={documents.length ? Math.round(documents.reduce((s, d) => s + d.days_since_upload, 0) / documents.length) : 0} />
      </div>

      {loading ? (
        <div className="space-y-3">{[1, 2, 3].map(i => <div key={i} className="h-16 bg-white rounded-lg animate-pulse" />)}</div>
      ) : (
        Object.entries(byType).map(([type, docs]) => (
          <div key={type} className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
            <div className="px-6 py-3 bg-gray-50 border-b border-gray-100 flex items-center gap-2">
              <FileText size={16} className="text-edwards-accent" />
              <h3 className="text-sm font-semibold text-edwards-slate">{type}</h3>
              <span className="text-xs text-edwards-gray ml-2">({docs.length} documents)</span>
            </div>
            <div className="divide-y divide-gray-100">
              {docs.map((d, i) => (
                <div key={i} className="px-6 py-3 flex items-center gap-4 hover:bg-edwards-light/30 transition-colors">
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-edwards-dark">{d.version}</span>
                      {d.amendment_number && d.amendment_number !== 'None' && (
                        <span className="text-xs px-1.5 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-200">
                          Amendment {d.amendment_number}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-3 mt-1 text-xs text-edwards-gray">
                      {d.site_name && d.site_name !== 'None' && (
                        <span className="flex items-center gap-1"><MapPin size={12} />{d.site_name}</span>
                      )}
                      {d.pi_name && d.pi_name !== 'None' && <span>PI: {d.pi_name}</span>}
                      <span>{d.tmf_zone}</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-edwards-gray">{d.days_since_upload}d old</span>
                    <span className={`text-xs font-semibold px-2 py-0.5 rounded border ${
                      d.completeness_status === 'Current'
                        ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                        : 'bg-amber-50 text-amber-700 border-amber-200'
                    }`}>
                      {d.completeness_status}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))
      )}
    </div>
  )
}

function StatCard({ icon: Icon, label, value, color }: { icon: any; label: string; value: number; color?: string }) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
      <div className="flex items-center gap-2 mb-1">
        <Icon size={16} className="text-edwards-accent" />
        <span className="text-xs text-edwards-gray uppercase tracking-wider">{label}</span>
      </div>
      <p className={`text-xl font-bold ${color || 'text-edwards-dark'}`}>{value}</p>
    </div>
  )
}
