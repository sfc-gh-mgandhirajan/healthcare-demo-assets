"use client"

import { useState } from "react"
import { KpiRibbon } from "@/components/kpi-ribbon"
import { InsightBlock } from "@/components/insight-block"
import { AgentDrawer } from "@/components/agent-drawer"
import { PanelCard } from "@/components/panel-card"
import {
  TrendForecastChart,
  PvmChart,
  SpecialtyStackChart,
  HighCostChart,
  ContributorsChart,
  BridgeChart,
  WasteConcentrationChart,
  RebateYieldChart,
  FormularyLeakageChart,
  PaOutcomesChart,
  AdherencePdcChart,
} from "@/components/charts"

type BandKey = "bandA" | "bandB" | "bandC" | "bandD"

const BANDS: { key: BandKey; n: string; title: string; tag: string; sub: string }[] = [
  { key: "bandA", n: "A", title: "Spend Performance & Outlook", tag: "trend vs plan", sub: "Where spend is and where it is heading" },
  { key: "bandB", n: "B", title: "Cost Drivers & Composition", tag: "where the dollars are", sub: "What is driving cost and how it composes" },
  { key: "bandC", n: "C", title: "Addressable Savings Opportunity", tag: "size of prize", sub: "The addressable prize and where it concentrates" },
  { key: "bandD", n: "D", title: "Cost Management Levers", tag: "yield & effectiveness", sub: "The levers you can pull and how they perform" },
]

export function CommandCenter() {
  const [band, setBand] = useState<BandKey>("bandA")
  const [agentOpen, setAgentOpen] = useState(false)
  const openAgent = () => setAgentOpen(true)
  const active = BANDS.find((b) => b.key === band)!

  return (
    <div className="app">
      <aside className="side">
        <div className="brand">
          <div className="dot" />
          <div>
            <b>
              Pharmacy Cost
              <br />
              Command Center
            </b>
            <span>Payer · Cost Intelligence</span>
          </div>
        </div>
        <nav className="nav">
          <div className="navsec">Bands</div>
          {BANDS.map((b) => (
            <button key={b.key} className={band === b.key ? "active" : ""} type="button" onClick={() => setBand(b.key)}>
              <span className="n">{b.n}</span>
              <span>{b.title}</span>
            </button>
          ))}
        </nav>
        <div className="foot">
          One certified pharmacy-cost book. Every band: the insight and the next governed move, with the agent a click away.
        </div>
      </aside>

      <main className="main">
        <div className="topbar">
          <div>
            <h1>Pharmacy Cost Command Center</h1>
            <div className="sub">One view: spend, the addressable prize, and the next governed moves</div>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <span className="pill">Commercial + MA</span>
            <span className="pill cert" title="Your data is certified and current">
              Certified data
            </span>
          </div>
        </div>

        <div className="wrap">
          <KpiRibbon />

          <div className="sechead">
            {active.title}
            <span className="stag">{active.tag}</span>
          </div>

          {band === "bandA" && (
            <div className="viz4">
              <PanelCard
                panelKey="trend"
                title="Trend & Forecast — Net Rx PMPM vs plan"
                hint="Monthly actual vs the plan line and the projected next-12-month trajectory (base + high)."
                height={250}
              >
                {(rows) => <TrendForecastChart rows={rows} />}
              </PanelCard>
              <PanelCard
                panelKey="pvm"
                title="Why it's moving — price vs utilization vs mix"
                hint="Decomposition of the YoY trend; utilization is the dominant lever."
                height={250}
              >
                {(rows) => <PvmChart rows={rows} />}
              </PanelCard>
            </div>
          )}

          {band === "bandB" && (
            <div className="viz4">
              <PanelCard
                panelKey="benefit"
                title="Specialty vs traditional — incl. medical-benefit spend"
                hint="Total drug spend split across traditional Rx, specialty pharmacy benefit, and specialty MEDICAL benefit (buy-and-bill) — the medical slice is invisible in Rx-only views."
                height={200}
              >
                {(rows) => <SpecialtyStackChart rows={rows} />}
              </PanelCard>
              <PanelCard
                panelKey="hicost"
                title="High-cost & emerging therapies — spend & growth"
                hint="Top classes by current net spend; hover for YoY growth."
                height={260}
              >
                {(rows) => <HighCostChart rows={rows} />}
              </PanelCard>
              <PanelCard
                span2
                panelKey="contrib"
                title="Top contributors to the trend — $ added YoY"
                hint="Dollars added year-over-year by therapeutic class, ranked by $ contribution (not % growth)."
                height={300}
              >
                {(rows) => <ContributorsChart rows={rows} />}
              </PanelCard>
            </div>
          )}

          {band === "bandC" && (
            <div className="viz4">
              <PanelCard
                panelKey="bridge"
                title="Addressable prize — by spend lever"
                hint="Each addressable dollar rolls up to one lever (one owner per dollar) — projected spend steps down to the clinically necessary floor with no double-count."
                height={300}
              >
                {(rows) => <BridgeChart rows={rows} />}
              </PanelCard>
              <PanelCard
                panelKey="waste"
                title="Where waste concentrates — the high-cost tail"
                hint="Share of net spend by claimant band; a small high-cost cohort carries a disproportionate share."
                height={300}
              >
                {(rows) => <WasteConcentrationChart rows={rows} />}
              </PanelCard>
            </div>
          )}

          {band === "bandD" && (
            <div className="viz4">
              <PanelCard
                panelKey="rebate"
                title="Formulary & rebates — rebate yield by class"
                hint="Rebates captured as a % of gross spend, lowest first. Single-source specialty captures the least."
                height={250}
              >
                {(rows) => <RebateYieldChart rows={rows} />}
              </PanelCard>
              <PanelCard
                panelKey="offform"
                title="Formulary leakage — off-formulary by drug"
                hint="Non-preferred drugs with preferred in-class alternatives, by steerable net spend."
                height={250}
              >
                {(rows) => <FormularyLeakageChart rows={rows} />}
              </PanelCard>
              <PanelCard
                panelKey="pa"
                title="Utilization management — prior-auth outcomes"
                hint="Approve / deny / pending mix across prior-auth decisions."
                height={230}
              >
                {(rows) => <PaOutcomesChart rows={rows} />}
              </PanelCard>
              <PanelCard
                panelKey="adherence"
                title="Adherence & avoidable waste — PDC by class"
                hint="Adherence (PDC) by therapeutic class vs the 0.80 threshold; lowest classes drive avoidable cost."
                height={230}
              >
                {(rows) => <AdherencePdcChart rows={rows} />}
              </PanelCard>
            </div>
          )}

          <InsightBlock bandKey={band} onAsk={openAgent} />
        </div>
      </main>

      <AgentDrawer open={agentOpen} onOpen={openAgent} onClose={() => setAgentOpen(false)} />
    </div>
  )
}
