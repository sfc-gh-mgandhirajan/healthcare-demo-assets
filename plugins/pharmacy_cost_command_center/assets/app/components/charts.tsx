"use client"

import {
  Bar,
  BarChart,
  Cell,
  LabelList,
  Line,
  ComposedChart,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import { CHART } from "@/lib/chart-theme"
import { num, moneyM, moneyFull, pct1, pct0, dec2, dollars2, signedPct1, intFmt } from "@/lib/format"
import { useKpi } from "@/lib/use-data"

const AXIS = { fontSize: 10, fill: CHART.txt }
const GRID = CHART.grid

/** Parse a Snowflake DATE ("2023-07-01" or ISO) into a "Jul '23" label without TZ drift. */
function fmtMonth(raw: unknown): string {
  const s = String(raw)
  const m = s.match(/(\d{4})-(\d{2})/)
  if (!m) return s
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
  return `${months[Number(m[2]) - 1]} '${m[1].slice(2)}`
}

function growthColor(g: number | null): string {
  if (g == null) return CHART.blue
  if (g >= 25) return CHART.bad
  if (g >= 12) return CHART.warn
  if (g < 0) return CHART.good
  return CHART.blue
}

/* ------------------------------------------------------------------ Band A */

export function TrendForecastChart({ rows }: { rows: Record<string, any>[] }) {
  const data = rows.map((r) => ({
    month: fmtMonth(r.MONTH),
    actual: num(r.PMPM_ACTUAL),
    plan: num(r.PMPM_PLAN),
    base: num(r.PMPM_FORECAST_BASE),
    high: num(r.PMPM_FORECAST_HIGH),
  }))
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={data} margin={{ top: 8, right: 12, left: 4, bottom: 4 }}>
        <XAxis dataKey="month" tick={AXIS} interval={5} tickLine={false} axisLine={{ stroke: GRID }} />
        <YAxis tick={AXIS} tickLine={false} axisLine={false} tickFormatter={(v) => `$${v}`} width={40} />
        <Tooltip
          formatter={(v: number, n: string) => [v == null ? "—" : dollars2(v), n]}
          contentStyle={{ fontSize: 11, borderRadius: 8, border: `1px solid ${GRID}` }}
        />
        <Line type="monotone" dataKey="plan" name="Plan" stroke={CHART.good} strokeWidth={2} dot={false} connectNulls />
        <Line type="monotone" dataKey="actual" name="Net PMPM" stroke={CHART.deep} strokeWidth={2} dot={false} connectNulls={false} />
        <Line type="monotone" dataKey="base" name="Forecast (base)" stroke={CHART.blue} strokeWidth={2} strokeDasharray="5 4" dot={false} connectNulls={false} />
        <Line type="monotone" dataKey="high" name="Forecast (high)" stroke={CHART.bad} strokeWidth={2} strokeDasharray="5 4" dot={false} connectNulls={false} />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

export function PvmChart({ rows }: { rows: Record<string, any>[] }) {
  const order: Record<string, number> = { Volume: 0, Price: 1, Mix: 2 }
  const colorFor: Record<string, string> = { Volume: CHART.blue, Price: CHART.deep, Mix: CHART.purple }
  const data = rows
    .map((r) => ({ label: String(r.COMPONENT), pct: num(r.PCT_OF_TREND), amt: num(r.AMOUNT) }))
    .sort((a, b) => (order[a.label] ?? 9) - (order[b.label] ?? 9))
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={data} margin={{ top: 4, right: 48, left: 12, bottom: 4 }}>
        <XAxis type="number" tick={AXIS} tickLine={false} axisLine={{ stroke: GRID }} tickFormatter={(v) => `${v}%`} />
        <YAxis type="category" dataKey="label" tick={AXIS} tickLine={false} axisLine={false} width={70} />
        <Tooltip formatter={(v: number) => [`${v}% of trend`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="pct" radius={[0, 3, 3, 0]} barSize={22}>
          {data.map((d, i) => (
            <Cell key={i} fill={colorFor[d.label] ?? CHART.blue} />
          ))}
          <LabelList dataKey="pct" position="right" formatter={(v: number) => `${v}%`} style={{ fontSize: 10, fill: CHART.ink }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

/* ------------------------------------------------------------------ Band B */

const BENEFIT_LABEL: Record<string, string> = {
  TRADITIONAL_RX: "Traditional (Rx)",
  SPECIALTY_PHARMACY: "Specialty — pharmacy benefit",
  SPECIALTY_MEDICAL: "Specialty — medical benefit",
}
const BENEFIT_COLOR: Record<string, string> = {
  TRADITIONAL_RX: CHART.blue,
  SPECIALTY_PHARMACY: CHART.warn,
  SPECIALTY_MEDICAL: CHART.bad,
}

export function SpecialtyStackChart({ rows }: { rows: Record<string, any>[] }) {
  const total = rows.reduce((s, r) => s + (num(r.NET_SPEND) ?? 0), 0)
  const datum: Record<string, any> = { name: "Total drug spend" }
  const order = ["TRADITIONAL_RX", "SPECIALTY_PHARMACY", "SPECIALTY_MEDICAL"]
  rows.forEach((r) => {
    datum[String(r.BENEFIT_TYPE)] = num(r.NET_SPEND)
  })
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={[datum]} margin={{ top: 30, right: 16, left: 16, bottom: 4 }} barSize={40}>
        <XAxis type="number" hide domain={[0, total]} />
        <YAxis type="category" dataKey="name" hide />
        <Tooltip
          formatter={(v: number, n: string) => [`${moneyM(v)} (${pct0((v / total) * 100)})`, BENEFIT_LABEL[n] ?? n]}
          contentStyle={{ fontSize: 11, borderRadius: 8 }}
        />
        {order.map((k) => (
          <Bar key={k} dataKey={k} stackId="s" fill={BENEFIT_COLOR[k]} name={BENEFIT_LABEL[k]}>
            <LabelList dataKey={k} position="center" formatter={(v: number) => (v / total > 0.06 ? pct0((v / total) * 100) : "")} style={{ fontSize: 11, fill: "#fff", fontWeight: 700 }} />
          </Bar>
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

export function HighCostChart({ rows }: { rows: Record<string, any>[] }) {
  const data = rows
    .slice(0, 8)
    .map((r) => ({ label: String(r.THERAPEUTIC_CLASS), val: num(r.NET_SPEND_CURRENT), g: num(r.YOY_GROWTH_PCT) }))
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={data} margin={{ top: 4, right: 64, left: 12, bottom: 4 }}>
        <XAxis type="number" tick={AXIS} tickLine={false} axisLine={{ stroke: GRID }} tickFormatter={(v) => moneyM(v)} />
        <YAxis type="category" dataKey="label" tick={{ fontSize: 9, fill: CHART.txt }} tickLine={false} axisLine={false} width={150} />
        <Tooltip formatter={(v: number, _n, p: any) => [`${moneyFull(v)} · ${signedPct1(p.payload.g)} YoY`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="val" radius={[0, 3, 3, 0]} barSize={16}>
          {data.map((d, i) => (
            <Cell key={i} fill={growthColor(d.g)} />
          ))}
          <LabelList dataKey="val" position="right" formatter={(v: number) => moneyM(v)} style={{ fontSize: 9.5, fill: CHART.deep }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

export function ContributorsChart({ rows }: { rows: Record<string, any>[] }) {
  const data = rows
    .slice(0, 10)
    .map((r) => ({ label: String(r.THERAPEUTIC_CLASS), val: num(r.DOLLARS_ADDED_YOY), driver: String(r.DOMINANT_DRIVER), g: num(r.YOY_GROWTH_PCT) }))
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={data} margin={{ top: 4, right: 64, left: 12, bottom: 4 }}>
        <XAxis type="number" tick={AXIS} tickLine={false} axisLine={{ stroke: GRID }} tickFormatter={(v) => moneyM(v)} />
        <YAxis type="category" dataKey="label" tick={{ fontSize: 9, fill: CHART.txt }} tickLine={false} axisLine={false} width={170} />
        <Tooltip formatter={(v: number, _n, p: any) => [`${moneyFull(v)} added · ${signedPct1(p.payload.g)} · ${p.payload.driver}-led`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="val" radius={[0, 3, 3, 0]} barSize={15}>
          {data.map((d, i) => (
            <Cell key={i} fill={i === 0 ? CHART.bad : i < 3 ? CHART.warn : i < 6 ? CHART.blue : CHART.deep} />
          ))}
          <LabelList dataKey="val" position="right" formatter={(v: number) => moneyM(v)} style={{ fontSize: 9.5, fill: CHART.deep }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

/* ------------------------------------------------------------------ Band C */

const LEVER_COLOR = [CHART.bad, CHART.purple, CHART.warn, CHART.orange]

export function BridgeChart({ rows }: { rows: Record<string, any>[] }) {
  const kpi = useKpi()
  const total = num(kpi.data?.projectedNext12Spend)
  const levers = [...rows].sort((a, b) => (num(a.LEVER_ORDER) ?? 0) - (num(b.LEVER_ORDER) ?? 0))
  if (total == null) {
    return <div className="loadingbox">Loading projected spend…</div>
  }
  // Build a waterfall: start at projected 12-mo, step down by each addressable lever, end at necessary floor.
  let running = total
  const bars: { label: string; base: number; delta: number; color: string; display: string; kind: string }[] = []
  bars.push({ label: "Projected 12-mo", base: 0, delta: total, color: CHART.deep, display: moneyM(total), kind: "total" })
  levers.forEach((r, i) => {
    const amt = num(r.ADDRESSABLE_AMOUNT) ?? 0
    const next = running - amt
    bars.push({
      label: String(r.LEVER_NAME),
      base: next,
      delta: amt,
      color: LEVER_COLOR[i % LEVER_COLOR.length],
      display: `−${moneyM(amt)}`,
      kind: "delta",
    })
    running = next
  })
  bars.push({ label: "Necessary floor", base: 0, delta: running, color: CHART.good, display: moneyM(running), kind: "floor" })

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={bars} margin={{ top: 22, right: 8, left: 4, bottom: 46 }}>
        <XAxis dataKey="label" tick={{ fontSize: 8.5, fill: CHART.txt }} interval={0} angle={-18} textAnchor="end" height={54} tickLine={false} axisLine={{ stroke: GRID }} />
        <YAxis tick={AXIS} tickLine={false} axisLine={false} tickFormatter={(v) => moneyM(v)} width={44} />
        <Tooltip formatter={(v: number, n: string) => (n === "delta" ? [moneyFull(v), "amount"] : ["", ""])} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="base" stackId="w" fill="transparent" />
        <Bar dataKey="delta" stackId="w" radius={[3, 3, 0, 0]}>
          {bars.map((b, i) => (
            <Cell key={i} fill={b.color} />
          ))}
          <LabelList dataKey="display" position="top" style={{ fontSize: 9.5, fill: CHART.ink }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

export function WasteConcentrationChart({ rows }: { rows: Record<string, any>[] }) {
  const order: Record<string, number> = { "Top 1%": 0, "Top 1-3%": 1, "Top 3-5%": 2, "Remaining 95%": 3 }
  const data = [...rows]
    .map((r) => ({ label: String(r.CLAIMANT_BAND), pct: num(r.PCT_OF_SPEND), members: num(r.MEMBERS), spend: num(r.NET_SPEND) }))
    .sort((a, b) => (order[a.label] ?? 9) - (order[b.label] ?? 9))
  const colors = [CHART.bad, CHART.warn, CHART.orange, CHART.good]
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={data} margin={{ top: 4, right: 52, left: 12, bottom: 4 }}>
        <XAxis type="number" tick={AXIS} tickLine={false} axisLine={{ stroke: GRID }} tickFormatter={(v) => `${v}%`} />
        <YAxis type="category" dataKey="label" tick={{ fontSize: 9.5, fill: CHART.txt }} tickLine={false} axisLine={false} width={120} />
        <Tooltip formatter={(v: number, _n, p: any) => [`${pct1(v)} of spend · ${intFmt(p.payload.members)} members · ${moneyFull(p.payload.spend)}`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="pct" radius={[0, 3, 3, 0]} barSize={20}>
          {data.map((_, i) => (
            <Cell key={i} fill={colors[i % colors.length]} />
          ))}
          <LabelList dataKey="pct" position="right" formatter={(v: number) => pct1(v)} style={{ fontSize: 10, fill: CHART.deep }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

/* ------------------------------------------------------------------ Band D */

export function RebateYieldChart({ rows }: { rows: Record<string, any>[] }) {
  const data = [...rows]
    .map((r) => ({ label: String(r.THERAPEUTIC_CLASS), val: num(r.REBATE_YIELD_PCT) }))
    .sort((a, b) => (a.val ?? 0) - (b.val ?? 0))
    .slice(0, 10)
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={data} margin={{ top: 4, right: 48, left: 12, bottom: 4 }}>
        <XAxis type="number" tick={AXIS} tickLine={false} axisLine={{ stroke: GRID }} tickFormatter={(v) => `${v}%`} />
        <YAxis type="category" dataKey="label" tick={{ fontSize: 9, fill: CHART.txt }} tickLine={false} axisLine={false} width={150} />
        <Tooltip formatter={(v: number) => [`${pct1(v)} rebate yield`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="val" radius={[0, 3, 3, 0]} barSize={15}>
          {data.map((d, i) => (
            <Cell key={i} fill={d.val != null && d.val < 12 ? CHART.bad : d.val != null && d.val < 25 ? CHART.warn : CHART.good} />
          ))}
          <LabelList dataKey="val" position="right" formatter={(v: number) => pct0(v)} style={{ fontSize: 9.5, fill: CHART.deep }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

export function FormularyLeakageChart({ rows }: { rows: Record<string, any>[] }) {
  const data = rows
    .slice(0, 8)
    .map((r) => ({ label: String(r.DRUG_NAME), val: num(r.STEERABLE_NET_SPEND), cls: String(r.THERAPEUTIC_CLASS) }))
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart layout="vertical" data={data} margin={{ top: 4, right: 56, left: 12, bottom: 4 }}>
        <XAxis type="number" tick={AXIS} tickLine={false} axisLine={{ stroke: GRID }} tickFormatter={(v) => moneyM(v)} />
        <YAxis type="category" dataKey="label" tick={{ fontSize: 9, fill: CHART.txt }} tickLine={false} axisLine={false} width={140} />
        <Tooltip formatter={(v: number, _n, p: any) => [`${moneyFull(v)} steerable · ${p.payload.cls}`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <Bar dataKey="val" radius={[0, 3, 3, 0]} barSize={16} fill={CHART.warn}>
          <LabelList dataKey="val" position="right" formatter={(v: number) => moneyM(v)} style={{ fontSize: 9.5, fill: CHART.deep }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

const PA_COLOR: Record<string, string> = { APPROVED: CHART.good, DENIED: CHART.bad, PENDING: CHART.warn }

export function PaOutcomesChart({ rows }: { rows: Record<string, any>[] }) {
  const data = rows.map((r) => ({ name: String(r.DECISION), value: num(r.PA_COUNT) ?? 0, pct: num(r.PCT_OF_TOTAL) }))
  return (
    <ResponsiveContainer width="100%" height="100%">
      <PieChart>
        <Pie data={data} dataKey="value" nameKey="name" innerRadius="55%" outerRadius="82%" paddingAngle={2} startAngle={90} endAngle={-270}>
          {data.map((d, i) => (
            <Cell key={i} fill={PA_COLOR[d.name] ?? CHART.blue} />
          ))}
        </Pie>
        <Tooltip formatter={(v: number, n: string, p: any) => [`${intFmt(v)} (${pct1(p.payload.pct)})`, n]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
      </PieChart>
    </ResponsiveContainer>
  )
}

export function AdherencePdcChart({ rows }: { rows: Record<string, any>[] }) {
  const threshold = num(rows[0]?.PDC_THRESHOLD) ?? 0.8
  const data = [...rows]
    .map((r) => ({ label: String(r.THERAPEUTIC_CLASS), pdc: num(r.AVG_PDC) }))
    .sort((a, b) => (a.pdc ?? 0) - (b.pdc ?? 0))
    .slice(0, 10)
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} margin={{ top: 8, right: 8, left: 4, bottom: 40 }}>
        <XAxis dataKey="label" tick={{ fontSize: 8, fill: CHART.txt }} interval={0} angle={-25} textAnchor="end" height={48} tickLine={false} axisLine={{ stroke: GRID }} />
        <YAxis tick={AXIS} tickLine={false} axisLine={false} domain={[0, 1]} tickFormatter={(v) => v.toFixed(1)} width={30} />
        <Tooltip formatter={(v: number) => [`PDC ${dec2(v)}`, ""]} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
        <ReferenceLine y={threshold} stroke={CHART.good} strokeDasharray="4 3" label={{ value: `${dec2(threshold)} target`, fontSize: 9, fill: CHART.good, position: "insideTopRight" }} />
        <Bar dataKey="pdc" radius={[3, 3, 0, 0]} barSize={26}>
          {data.map((d, i) => (
            <Cell key={i} fill={d.pdc != null && d.pdc < threshold ? (d.pdc < 0.5 ? CHART.bad : CHART.warn) : CHART.good} />
          ))}
          <LabelList dataKey="pdc" position="top" formatter={(v: number) => dec2(v)} style={{ fontSize: 8.5, fill: CHART.ink }} />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}
