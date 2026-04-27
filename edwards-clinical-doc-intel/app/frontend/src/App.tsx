import { useState } from 'react'
import TrialPortfolio from './components/TrialPortfolio'
import TrialDrilldown from './components/TrialDrilldown'
import AgentChat from './components/AgentChat'
import AuditReport from './components/AuditReport'
import { LayoutGrid, MessageSquare, FileText, Activity } from 'lucide-react'

type Screen = 'portfolio' | 'drilldown' | 'chat' | 'audit'

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

export default function App() {
  const [screen, setScreen] = useState<Screen>('portfolio')
  const [selectedTrial, setSelectedTrial] = useState<Trial | null>(null)
  const [chatKey, setChatKey] = useState(0)

  const handleNav = (id: Screen) => {
    if (id === 'chat' && screen === 'chat') {
      setChatKey(k => k + 1)
    }
    setScreen(id)
  }

  const handleTrialSelect = (trial: Trial) => {
    setSelectedTrial(trial)
    setScreen('drilldown')
  }

  const handleAuditRequest = (trialName: string) => {
    setSelectedTrial(prev => prev?.trial_name === trialName ? prev : { trial_name: trialName } as Trial)
    setScreen('audit')
  }

  const nav = [
    { id: 'portfolio' as Screen, label: 'Trial Portfolio', icon: LayoutGrid },
    { id: 'drilldown' as Screen, label: 'Trial Detail', icon: Activity },
    { id: 'chat' as Screen, label: 'AI Assistant', icon: MessageSquare },
    { id: 'audit' as Screen, label: 'Audit Reports', icon: FileText },
  ]

  return (
    <div className="min-h-screen bg-edwards-light">
      <header className="bg-edwards-dark text-white shadow-lg">
        <div className="max-w-[1400px] mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="w-8 h-8 bg-edwards-red rounded flex items-center justify-center font-bold text-sm">EW</div>
            <div>
              <h1 className="text-lg font-semibold tracking-tight">TMF Governance Console</h1>
              <p className="text-xs text-gray-400">Edwards Lifesciences | Clinical Document Intelligence</p>
            </div>
          </div>
          <div className="flex items-center gap-1 bg-white/10 rounded-lg p-1">
            {nav.map(n => (
              <button
                key={n.id}
                onClick={() => handleNav(n.id)}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-sm transition-all ${
                  screen === n.id
                    ? 'bg-white text-edwards-dark font-medium shadow-sm'
                    : 'text-gray-300 hover:text-white hover:bg-white/10'
                }`}
              >
                <n.icon size={16} />
                <span className="hidden md:inline">{n.label}</span>
              </button>
            ))}
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-400">Powered by</span>
            <span className="text-xs font-medium text-edwards-accent">Snowflake Cortex</span>
          </div>
        </div>
      </header>

      <main className="max-w-[1400px] mx-auto px-6 py-6">
        {screen === 'portfolio' && <TrialPortfolio onTrialSelect={handleTrialSelect} />}
        {screen === 'drilldown' && <TrialDrilldown trial={selectedTrial} onAuditRequest={handleAuditRequest} onBack={() => setScreen('portfolio')} />}
        {screen === 'chat' && <AgentChat key={chatKey} />}
        {screen === 'audit' && <AuditReport trialName={selectedTrial?.trial_name} />}
      </main>
    </div>
  )
}
