"use client"
import React, { useMemo, useState } from "react"
import type { CommandCenterData, Row } from "@/lib/data"
import { AgentRail } from "@/components/agent-rail"
import { MomentumChart } from "@/components/momentum-chart"
import {
  BulletBar, BarH, SparkGrid, FunnelH, DivBar, DeviationBar, HWaterfall,
  PAL, fmtPct, fmtPts, fmtMins, fmtNum, type BulletRow, type BarRow, type SparkItem,
} from "@/components/charts"

const num = (v: unknown) => { const n = Number(v); return Number.isFinite(n) ? n : 0 }
const usd = (v: number) => v >= 1e6 ? `$${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `$${(v / 1e3).toFixed(0)}K` : `$${Math.round(v)}`

/** Quarter label like "Q3 '25" from a date-ish value. */
function qLabel(q: unknown): string {
  const d = new Date(String(q))
  if (isNaN(d.getTime())) return String(q)
  return `Q${Math.floor(d.getUTCMonth() / 3) + 1} '${String(d.getUTCFullYear()).slice(2)}`
}

/** Normalize a Snowflake ARRAY column (already-parsed array or JSON string) to string[]. */
function toTags(v: unknown): string[] {
  if (Array.isArray(v)) return v.map(String)
  if (typeof v === "string" && v.trim()) {
    try { const p = JSON.parse(v); if (Array.isArray(p)) return p.map(String) } catch { /* not json */ }
    return v.split(",").map((s) => s.trim()).filter(Boolean)
  }
  return []
}

/** Inline markdown: **bold** and *italic* / _italic_ → React nodes (XSS-safe, no dangerouslySetInnerHTML). */
function mdInline(s: string, keyBase: string): React.ReactNode[] {
  const out: React.ReactNode[] = []
  const re = /\*\*([^*]+)\*\*|\*([^*]+)\*|_([^_]+)_/g
  let last = 0, m: RegExpExecArray | null, i = 0
  while ((m = re.exec(s)) !== null) {
    if (m.index > last) out.push(s.slice(last, m.index))
    if (m[1] != null) out.push(<strong key={`${keyBase}-b${i}`}>{m[1]}</strong>)
    else out.push(<em key={`${keyBase}-e${i}`}>{m[2] ?? m[3]}</em>)
    last = m.index + m[0].length; i++
  }
  if (last < s.length) out.push(s.slice(last))
  return out
}

/** Minimal GFM renderer for the insight body: bullet lists, paragraphs, inline bold/italic. */
function MiniMarkdown({ text }: { text: string }) {
  let t = (text ?? "").replace(/\r/g, "")
  if (!t.includes("\n")) t = t.replace(/\s+-\s+/g, "\n- ") // fold single-line bullet strings
  const blocks: React.ReactNode[] = []
  let bullets: string[] = []
  let n = 0
  const flush = () => {
    if (bullets.length) {
      const items = bullets
      blocks.push(<ul key={`ul${n++}`}>{items.map((b, j) => <li key={j}>{mdInline(b, `u${n}-${j}`)}</li>)}</ul>)
      bullets = []
    }
  }
  for (const raw of t.split("\n")) {
    const line = raw.trim()
    if (!line) { flush(); continue }
    const mb = line.match(/^[-*]\s+(.*)$/)
    if (mb) { bullets.push(mb[1]); continue }
    flush()
    blocks.push(<p key={`p${n++}`}>{mdInline(line, `p${n}`)}</p>)
  }
  flush()
  return <>{blocks}</>
}

/** Score band → color class (higher is better). */
function band(v: number): "good" | "warn" | "bad" { return v >= 78 ? "good" : v >= 68 ? "warn" : "bad" }
function bandColor(v: number): string { const b = band(v); return b === "good" ? PAL.good : b === "warn" ? PAL.warn : PAL.bad }

const STAGE_META: Record<string, { emoji: string; sub: string }> = {
  s1: { emoji: "🚪", sub: "access experience" },
  s2: { emoji: "🤝", sub: "communication" },
  s3: { emoji: "🛏️", sub: "responsiveness" },
  s4: { emoji: "📋", sub: "care transition" },
  s5: { emoji: "🏡", sub: "would recommend" },
}

const COMM_CODES = ["H_COMP_1", "H_COMP_2", "H_COMP_3", "H_COMP_5"]

export function CommandCenter({ data }: { data: CommandCenterData }) {
  const [stage, setStage] = useState("s1")
  const [railOpen, setRailOpen] = useState(false)
  const [seed, setSeed] = useState<string | null>(null)
  const [seedNonce, setSeedNonce] = useState(0)

  const k = data.kpis
  const journey = data.journey

  const stageName = useMemo(() => {
    const row = journey.find((r) => r.STAGE_KEY === stage)
    return (row?.STAGE_NAME as string) ?? STAGE_META[stage]?.sub ?? stage
  }, [journey, stage])

  function openAgent() { setRailOpen(true) }
  function askAgent(q: string) { setSeed(q); setSeedNonce((n) => n + 1); setRailOpen(true) }

  return (
    <>
      <div className="topbar">
        <div className="brand">
          <div className="dot">♥</div>
          <div>
            <b>Patient Experience Command Center</b>
            <span>For the Chief Experience Officer &amp; the Patient Experience team</span>
          </div>
        </div>
        <div className="pills">
          <a className="pill logout" href="/sfc-endpoint/logout" title="Sign out of this app">
            <span className="lo-ico" aria-hidden="true">⏻</span> Log out
          </a>
        </div>
      </div>

      <div className="wrap">
        {data.errors.length > 0 && (
          <div className="errbanner">
            <b>Some panels could not load.</b> {data.errors.slice(0, 3).join(" · ")}
          </div>
        )}

        <KpiRibbon k={k} />
        <JourneyNav journey={journey} stage={stage} onPick={setStage} />

        <StageDetail
          stage={stage}
          stageName={stageName}
          data={data}
          onAsk={askAgent}
        />

        <div className="foot">
          One certified patient-experience record, told as the patient lives it — HCAHPS and Voice-of-Patient AI.
        </div>
      </div>

      <button className={`agentfab${railOpen ? " hidden" : ""}`} onClick={openAgent}>
        <span className="spark" />Ask the agent
      </button>
      <AgentRail
        open={railOpen}
        onClose={() => setRailOpen(false)}
        stageKey={stage}
        stageName={stageName}
        seedQuestion={seed}
        seedNonce={seedNonce}
      />
    </>
  )
}

function KpiRibbon({ k }: { k: CommandCenterData["kpis"] }) {
  if (!k) return null
  const composite = num(k.COMPOSITE_TOPBOX)
  const yoy = num(k.COMPOSITE_YOY_DELTA_PTS)
  const star = num(k.STAR_LEVEL)
  const netRev = num(k.NET_INPATIENT_REVENUE)
  const vbpPct = netRev > 0 ? Math.round((num(k.VBP_AT_RISK_USD) / netRev) * 1000) / 10 : null
  const starStr = "★★★★★".slice(0, star)
  const offStr = "★★★★★".slice(0, 5 - star)
  return (
    <div className="ribbon">
      <div className="kpi">
        <div className="lab">HCAHPS Star (Hospital Compare)</div>
        <div className="val"><span className="stars">{starStr}<span className="off">{offStr}</span></span></div>
        <div className="delta flat">{star} of 5 · ~{fmtPts(num(k.PTS_TO_NEXT_STAR))} top-box to next star</div>
      </div>
      <div className="kpi">
        <div className="lab">Composite (top-box)</div>
        <div className="val">{fmtPct(composite)}</div>
        <div className={`delta ${yoy < 0 ? "down" : yoy > 0 ? "up" : "flat"}`}>{yoy < 0 ? "▼" : yoy > 0 ? "▲" : "▬"} {fmtPts(Math.abs(yoy))} YoY</div>
      </div>
      <div className="kpi warnborder">
        <div className="lab">VBP payment at risk</div>
        <div className="val" style={{ color: "var(--bad)" }}>{usd(num(k.VBP_AT_RISK_USD))}</div>
        <div className="delta down">{vbpPct != null ? `▼ ≈${vbpPct}% of net inpatient revenue` : "▼ at risk"}</div>
      </div>
      <div className="kpi warnborder">
        <div className="lab">Patients awaiting recovery</div>
        <div className="val" style={{ color: "var(--bad)" }}>{num(k.PATIENTS_AWAITING_RECOVERY)}</div>
        <div className="delta flat">{num(k.FORMAL_GRIEVANCES_OPEN)} formal grievances</div>
      </div>
    </div>
  )
}

function JourneyNav({ journey, stage, onPick }: { journey: Row[]; stage: string; onPick: (s: string) => void }) {
  return (
    <div className="journeywrap">
      <div className="jhead">The patient journey — where experience is won and lost</div>
      <div className="journey">
        {journey.map((r) => {
          const key = r.STAGE_KEY as string
          const score = num(r.SCORE_PCT)
          const b = band(score)
          const qoq = num(r.QOQ_DELTA)
          const meta = STAGE_META[key] ?? { emoji: "•", sub: "" }
          return (
            <button key={key} className={`jstage${stage === key ? " active" : ""}`} onClick={() => onPick(key)}>
              <div className={`jnode ${b}`}>{meta.emoji}</div>
              <div className="jname">{r.STAGE_NAME}</div>
              <div className={`jscore ${b}`}>{Math.round(score)}%</div>
              <div className="jsub">{meta.sub}</div>
              <div className={`jtrend ${qoq < 0 ? "down" : qoq > 0 ? "up" : "flat"}`}>
                {qoq < 0 ? "▼" : qoq > 0 ? "▲" : "▬"} {r.TREND ?? (qoq < 0 ? "declining" : qoq > 0 ? "improving" : "steady")}
              </div>
            </button>
          )
        })}
      </div>
    </div>
  )
}

function StageDetail({ stage, stageName, data, onAsk }: { stage: string; stageName: string; data: CommandCenterData; onAsk: (q: string) => void }) {
  const meta = STAGE_META[stage] ?? { emoji: "•", sub: "" }
  const jrow = data.journey.find((r) => r.STAGE_KEY === stage)
  const score = num(jrow?.SCORE_PCT)
  const insight = data.tabInsights.find((r) => (r.STAGE_NAME as string) === stageName || num(r.STAGE_ORDER) === num(jrow?.STAGE_ORDER))

  return (
    <section>
      <div className="stagehead">
        <div>
          <h2 className="st-title"><span className="em">{meta.emoji}</span>{stageName}</h2>
          <div className="st-sub">{STAGE_SUBTITLE[stage]}</div>
        </div>
        <div className="st-score"><div className={`n ${band(score)}`}>{Math.round(score)}%</div><div className="l">{meta.sub}</div></div>
      </div>

      {stage === "s1" && <StageS1 data={data} />}
      {stage === "s2" && <StageS2 data={data} />}
      {stage === "s3" && <StageS3 data={data} />}
      {stage === "s4" && <StageS4 data={data} />}
      {stage === "s5" && <StageS5 data={data} />}

      <VoicePanel data={data} stageName={stageName} />

      {insight && (
        <div className="insights">
          <div className="ihead">
            <span className="aibadge"><span className="spark2" />AI-powered</span>
            <b>AI Powered Insights</b>
          </div>
          <div className="agentmsg">
            <div className="amhead"><span className="amdot" />{(insight.AGENT_PERSONA as string) || `Experience Agent · ${stageName}`}</div>
            <div className="ambody">
              <MiniMarkdown text={(insight.INSIGHT_MD as string) || (insight.INSIGHT as string) || ""} />
            </div>
            <div className="itags">
              {toTags(insight.TAGS).map((t) => (
                <span key={t} className={`tagi ${t.toLowerCase()}`}>{t.toUpperCase()}</span>
              ))}
              <button className="tagi ask" onClick={() => onAsk(`For ${stageName}, what governed action should we take this week and who should own it?`)}>Ask the agent →</button>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}

const STAGE_SUBTITLE: Record<string, string> = {
  s1: "The first impression — getting in, getting seen, getting cared for on time",
  s2: "Feeling heard — how nurses and doctors communicate at the bedside",
  s3: "The heart of the stay — being there when the call light goes on",
  s4: "Leaving ready — the biggest detractor, and the one most tied to readmission",
  s5: "After the stay — closing the loop, recovering trust, earning the recommendation",
}

/* ---------- Stage panels ---------- */

function Card({ title, hint, info, children }: { title: string; hint: string; info?: string; children: React.ReactNode }) {
  return (
    <div className={`card${info ? " hasinfo" : ""}`}>
      {info && (
        <span className="infoDot" role="img" aria-label="About this chart">i<span className="tip"><b>{title}.</b> {info}</span></span>
      )}
      <h3>{title}</h3>
      <p className="hint">{hint}</p>
      <div className="chartbox">{children}</div>
    </div>
  )
}

function StageS1({ data }: { data: CommandCenterData }) {
  const wait: BulletRow[] = data.waitByArea.map((r) => {
    const value = num(r.AVG_WAIT_MIN), target = num(r.TARGET_MIN)
    const color = target && value <= target ? PAL.good : target && value <= target * 1.3 ? PAL.warn : PAL.bad
    return { label: String(r.AREA), value, target: target || undefined, color }
  })
  return (
    <div className="viz2">
      <Card title="Wait vs target by area" hint="Average wait against each area's target (black tick = target; minutes). The leading access detractors before care even begins." info="Average wait minutes by care area from visit-timing and encounter events, compared with each area's operational target (black tick).">
        {wait.length ? <BulletBar rows={wait} fmt={fmtMins} /> : <Empty />}
      </Card>
      <Card title="Access experience momentum" hint="On-time access % (left axis) and LWBS rate % (right axis) by quarter — hover a quarter for exact values." info="On-time access top-box and left-without-being-seen (LWBS) rate over recent quarters, derived from arrival and timing events.">
        {data.accessMomentum.length ? <MomentumChart rows={data.accessMomentum} /> : <Empty />}
      </Card>
    </div>
  )
}

function StageS2({ data }: { data: CommandCenterData }) {
  const comm: BulletRow[] = data.measureBenchmark
    .filter((r) => COMM_CODES.includes(String(r.MEASURE_CODE)))
    .map((r) => {
      const value = num(r.TOPBOX_PCT), target = num(r.TARGET_TOPBOX), benchmark = num(r.NATIONAL_AVG_TOPBOX)
      const color = value >= target ? PAL.good : value >= target * 0.9 ? PAL.warn : PAL.bad
      return { label: String(r.MEASURE_NAME), value, target: target || undefined, benchmark: benchmark || undefined, color }
    })
  const codes = Array.from(new Set(data.measureTrend.filter((r) => COMM_CODES.includes(String(r.MEASURE_CODE))).map((r) => String(r.MEASURE_CODE))))
  const nameOf = (code: string) => data.measureBenchmark.find((r) => r.MEASURE_CODE === code)?.MEASURE_NAME ?? code
  const palette = [PAL.deep, PAL.blue, PAL.good, PAL.warn]
  const trend: SparkItem[] = codes.map((code, i) => {
    const rowsFor = data.measureTrend.filter((r) => r.MEASURE_CODE === code)
    return {
      name: String(nameOf(code)),
      color: palette[i % palette.length],
      data: rowsFor.map((r) => num(r.TOPBOX_PCT)),
      labels: rowsFor.map((r) => qLabel(r.QUARTER)),
      fmt: fmtPct,
    }
  })
  const equity: BarRow[] = data.equityGap.map((r) => ({ label: String(r.RACE), value: num(r.GAP_PTS) }))
  return (
    <>
      <div className="viz2">
        <Card title="Communication top-box vs target & benchmark" hint="Top-box by measure (black tick = target; dashed = national median)." info="HCAHPS survey top-box by communication measure vs the target (black tick) and the national median (dashed line).">
          {comm.length ? <BulletBar rows={comm} fmt={fmtPct} forcedMax={100} /> : <Empty />}
        </Card>
        <Card title="Communication subdomains — momentum" hint="Each communication measure over recent quarters." info="HCAHPS top-box for each communication subdomain over recent quarters — hover a point for the exact value.">
          {trend.length ? <SparkGrid items={trend} /> : <Empty />}
        </Card>
      </div>
      <div style={{ marginBottom: 16 }}>
        <Card title="Equity lens — communication top-box gap vs house average" hint="Nurse-communication top-box by patient race/ethnicity, as points above or below the house average. Scored on cells with sufficient responses." info="Nurse-communication top-box by patient race/ethnicity, shown as points above or below the house average. Only cells with enough responses are scored.">
          {equity.length ? <DivBar rows={equity} fmt={fmtPts} /> : <Empty />}
        </Card>
      </div>
    </>
  )
}

function StageS3({ data }: { data: CommandCenterData }) {
  const drivers: BarRow[] = data.driverImportance.map((r) => {
    const value = num(r.IMPORTANCE)
    const color = value >= 0.5 ? PAL.bad : value >= 0.3 ? PAL.warn : PAL.blue
    return { label: String(r.DRIVER), value, color }
  })
  const houseAvg = num(data.kpis?.COMPOSITE_TOPBOX)
  const units: BarRow[] = data.unitScorecard.map((r) => {
    const value = num(r.COMPOSITE_TOPBOX)
    return { label: String(r.UNIT_NAME ?? r.UNIT_ID), value, color: bandColor(value) }
  })
  return (
    <div className="viz2">
      <Card title="What's driving responsiveness down" hint="Operational drivers ranked by composite points at stake (from the driver model)." info="Operational drivers ranked by the composite top-box points at stake, from the driver-importance model.">
        {drivers.length ? <BarH rows={drivers} fmt={fmtPts} /> : <Empty />}
      </Card>
      <Card title="Composite by unit" hint="Composite top-box by unit as points above or below the house average (center line). Units left of center (red) are the ones to round on first." info="HCAHPS composite top-box per unit shown as points above or below the house average (center line). Units left of center trail the house.">
        {units.length ? <DeviationBar rows={units} houseAvg={houseAvg} fmt={fmtPct} /> : <Empty />}
      </Card>
    </div>
  )
}

function StageS4({ data }: { data: CommandCenterData }) {
  const k = data.kpis
  const endV = num(k?.COMPOSITE_TOPBOX)
  const delta = num(k?.COMPOSITE_YOY_DELTA_PTS)
  const startV = endV - delta
  const steps = data.pointsLost
    .slice()
    .sort((a, b) => num(a.WEIGHTED_CONTRIB_PTS) - num(b.WEIGHTED_CONTRIB_PTS))
    .map((r) => ({ label: String(r.DOMAIN_NAME ?? r.MEASURE_CODE).slice(0, 16), value: num(r.WEIGHTED_CONTRIB_PTS) }))
  const funnel = data.dischargeReadiness.map((r) => ({ label: String(r.STEP), value: num(r.RATE_PCT), target: num(r.TARGET_PCT) }))
  return (
    <div className="viz2">
      <Card title="How the composite moved (YoY)" hint="Composite top-box bridged from last year to now; measures that barely moved are grouped as 'Other'. The measures driving most of the drop." info="Year-over-year composite top-box bridged by each domain's weighted contribution; near-zero measures are grouped as 'Other'.">
        {steps.length ? <HWaterfall startV={startV} steps={steps} endV={endV} fmt={fmtPct} /> : <Empty />}
      </Card>
      <Card title="Discharge readiness & closing the loop" hint="Transition-experience rates vs target (edge ticks) — the levers tied to readmission." info="Care-transition and close-the-loop rates (follow-up scheduled, patient reached, issue resolved) vs target, from callbacks and appointments.">
        {funnel.length ? <FunnelH steps={funnel} fmt={fmtPct} /> : <Empty />}
      </Card>
    </div>
  )
}

function StageS5({ data }: { data: CommandCenterData }) {
  const aging: BarRow[] = data.grievanceAging.map((r) => {
    const order = num(r.BUCKET_ORDER)
    const color = order === 1 ? PAL.bad : order === 2 ? PAL.bad : order === 3 ? PAL.warn : PAL.good
    return { label: String(r.AGING_BUCKET), value: num(r.N_GRIEVANCES), color }
  })
  const sla: BulletRow[] = data.recoverySla.map((r) => {
    const value = num(r.ADHERENCE_PCT), target = num(r.TARGET_PCT)
    const color = value >= target ? PAL.good : value >= target * 0.85 ? PAL.warn : PAL.bad
    return { label: String(r.SLA_METRIC), value, target: target || undefined, color }
  })
  return (
    <div className="viz2">
      <Card title="Grievance aging vs regulatory window" hint="Formal grievances by time-to-response deadline. Overdue and due-within-48h are the compliance risk." info="Open formal grievances bucketed by time remaining to their regulatory response deadline; overdue and due-soon buckets are the compliance risk.">
        {aging.length ? <BarH rows={aging} fmt={fmtNum} /> : <Empty />}
      </Card>
      <Card title="Service-recovery SLA adherence" hint="Closed-within-window rates vs SLA target (black tick)." info="Share of grievances and callbacks closed within their SLA window vs the target (black tick).">
        {sla.length ? <BulletBar rows={sla} fmt={fmtPct} forcedMax={100} /> : <Empty />}
      </Card>
    </div>
  )
}

function Empty() { return <div style={{ fontSize: 12, color: "var(--ink-soft)", padding: "24px 0", textAlign: "center" }}>No data available.</div> }

/* ---------- Voice of Patient ---------- */

function VoicePanel({ data, stageName }: { data: CommandCenterData; stageName: string }) {
  const exact = data.voiceThemes.filter((r) => String(r.JOURNEY_STAGE) === stageName)
  const rows = exact.length ? exact : data.voiceThemes
  const total = data.voiceThemes.reduce((s, r) => s + num(r.MENTIONS), 0)
  return (
    <div className="voicewrap">
      <div className="voicehead"><span className="vb">🗣 Voice-of-Patient · AI</span> sentiment by experience theme — from the governed patient-voice index</div>
      <div className="vmeta">Analyzed <b>{total.toLocaleString()}</b> patient comments across themes — sentiment is aggregated per theme (net sentiment, volume), not cherry-picked quotes. Verbatims are cited drill-down inside the agent.</div>
      <div className="vsa">
        <div className="vhrow"><div>Experience theme</div><div>AI sentiment · volume</div><div>Net</div></div>
        {rows.length === 0 && <div className="vrow"><div className="reason">No themes for this stage.</div></div>}
        {rows.map((r, i) => {
          const net = num(r.NET_SENTIMENT)
          const dot = net >= 20 ? "good" : net >= -10 ? "warn" : "bad"
          const pos = num(r.PCT_POSITIVE), neu = num(r.PCT_NEUTRAL), neg = num(r.PCT_NEGATIVE)
          return (
            <div className="vrow" key={i}>
              <div className="vcat"><span className={`vdot ${dot}`} /><b>{r.THEME}</b></div>
              <div>
                <div className="barwrap">
                  <div className="vbar"><i className="pos" style={{ width: `${pos}%` }} /><i className="neu" style={{ width: `${neu}%` }} /><i className="neg" style={{ width: `${neg}%` }} /></div>
                  <div className="vpop">
                    <div className="pt">AI sentiment across {num(r.MENTIONS).toLocaleString()} comments</div>
                    <div className="legrow"><span className="sw pos" />Positive <span className="val">{Math.round(pos)}%</span></div>
                    <div className="legrow"><span className="sw neu" />Neutral <span className="val">{Math.round(neu)}%</span></div>
                    <div className="legrow"><span className="sw neg" />Negative <span className="val">{Math.round(neg)}%</span></div>
                    <div className="defn"><b>Net {net > 0 ? "+" : ""}{Math.round(net)}</b> = %positive − %negative (range −100 to +100). {net < 0 ? "Below 0 means comments skew negative." : net > 0 ? "Above 0 means comments skew positive." : "Around 0 means mixed sentiment."}</div>
                  </div>
                </div>
                <div className="vsentmeta"><span className="vvol">{num(r.MENTIONS).toLocaleString()} mentions</span></div>
              </div>
              <div className={`vnet ${dot}`}>{net > 0 ? "+" : ""}{Math.round(net)} net</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
