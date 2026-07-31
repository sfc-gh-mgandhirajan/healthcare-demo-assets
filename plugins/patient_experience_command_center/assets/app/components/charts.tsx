"use client"
import React, { useRef, useState } from "react"

/** Palette shared across charts (matches the mockup). */
export const PAL = {
  blue: "#29B5E8",
  deep: "#1D5F8A",
  good: "#2E9E6B",
  warn: "#d98b1a",
  bad: "#e5484d",
  grid: "#e6edf3",
  txt: "#5b6b7a",
  ink: "#11242f",
}

export const fmtPct = (v: number) => `${Math.round(v * 10) / 10}%`
export const fmtPts = (v: number) => `${Math.round(v * 10) / 10} pts`
export const fmtMins = (v: number) => `${Math.round(v)}m`
export const fmtNum = (v: number) => `${Math.round(v)}`

/** Signed value using the chart's own formatter (e.g. +2.1%, -3 pts). */
const signed = (v: number, fmt: (n: number) => string) => `${v > 0 ? "+" : v < 0 ? "-" : ""}${fmt(Math.abs(v))}`

const W = 560
const H = 220

function Svg({ h = H, children }: { h?: number; children: React.ReactNode }) {
  return (
    <svg viewBox={`0 0 ${W} ${h}`} width="100%" preserveAspectRatio="xMidYMid meet" style={{ display: "block", height: "auto" }}>
      {children}
    </svg>
  )
}

/* ---------- shared hover-for-detail tooltip (wrapper onMouseMove + hit zones) ---------- */

type TipLine = { label: string; val: string; color?: string }
/** A hover zone in viewBox units, plus its tooltip anchor (cx,cy) and content. */
interface Zone { x0: number; x1: number; y0: number; y1: number; cx: number; cy: number; flip?: boolean; below?: boolean; node: React.ReactNode }
interface Tip { leftPct: number; topPct: number; flip: boolean; below: boolean; node: React.ReactNode }

function TipBody({ title, lines, foot }: { title: string; lines: TipLine[]; foot?: string }) {
  return (
    <>
      <div style={{ fontSize: 12.5, fontWeight: 800, color: "var(--ink)", marginBottom: 4 }}>{title}</div>
      {lines.map((l, i) => (
        <div key={i} style={{ fontSize: 12, color: l.color || PAL.deep, marginBottom: 2 }}>{l.label} : <b>{l.val}</b></div>
      ))}
      {foot && <div style={{ fontSize: 10.5, color: "var(--ink-soft)", marginTop: 3 }}>{foot}</div>}
    </>
  )
}

function TipCard({ tip }: { tip: Tip }) {
  const tx = tip.flip ? "translateX(-100%) translateX(-6px)" : "translateX(6px)"
  const ty = tip.below ? "translateY(8px)" : "translateY(-100%) translateY(-8px)"
  return (
    <div style={{
      position: "absolute", left: `${tip.leftPct}%`, top: `${tip.topPct}%`,
      transform: `${tx} ${ty}`,
      background: "#fff", border: "1px solid var(--line)", borderRadius: 10,
      boxShadow: "0 8px 22px rgba(17,86,127,.16)", padding: "8px 11px",
      pointerEvents: "none", minWidth: 132, zIndex: 5, whiteSpace: "nowrap",
    }}>
      {tip.node}
    </div>
  )
}

/**
 * Relative wrapper that renders the SVG plus a floating tooltip.
 * Hover is tracked with a single wrapper-level onMouseMove (like MomentumChart):
 * the cursor is mapped into viewBox units and matched to a hit zone.
 */
function ChartFrame({ h = H, zones, children }: { h?: number; zones: Zone[]; children: React.ReactNode }) {
  const [tip, setTip] = useState<Tip | null>(null)
  const ref = useRef<HTMLDivElement>(null)
  function onMove(e: React.MouseEvent) {
    const rect = ref.current?.getBoundingClientRect(); if (!rect || !zones.length) return
    const fx = (e.clientX - rect.left) / rect.width * W
    const fy = (e.clientY - rect.top) / rect.height * h
    let z: Zone | undefined = zones.find((zz) => fx >= zz.x0 && fx <= zz.x1 && fy >= zz.y0 && fy <= zz.y1)
    if (!z) {
      let best: Zone | undefined, bd = Infinity
      for (const zz of zones) { const dx = (zz.x0 + zz.x1) / 2 - fx, dy = (zz.y0 + zz.y1) / 2 - fy; const d = dx * dx + dy * dy; if (d < bd) { bd = d; best = zz } }
      z = best
    }
    if (!z) { setTip(null); return }
    setTip({ leftPct: z.cx / W * 100, topPct: z.cy / h * 100, flip: z.flip ?? (z.cx / W > 0.6), below: z.below ?? (z.cy / h < 0.22), node: z.node })
  }
  return (
    <div ref={ref} style={{ position: "relative" }} onMouseMove={onMove} onMouseLeave={() => setTip(null)}>
      <Svg h={h}>{children}</Svg>
      {tip && <TipCard tip={tip} />}
    </div>
  )
}

export interface BulletRow { label: string; value: number; target?: number; benchmark?: number; color?: string }

/** Horizontal bullet bar: value bar + solid target tick + optional dashed benchmark tick. */
export function BulletBar({ rows, fmt, forcedMax, h = H }: { rows: BulletRow[]; fmt: (v: number) => string; forcedMax?: number; h?: number }) {
  const padL = 176, padR = 52, padT = 10, padB = 16
  const chartH = h - padT - padB, chartW = W - padL - padR
  let maxv = forcedMax || 0
  if (!forcedMax) { rows.forEach((r) => { const m = Math.max(r.value, r.target || 0, r.benchmark || 0); if (m > maxv) maxv = m }); maxv = maxv * 1.14 }
  const rowH = chartH / Math.max(1, rows.length), bh = Math.min(16, rowH * 0.4)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  for (let i = 0; i <= 4; i++) { const x = padL + chartW * i / 4; els.push(<line key={`g${i}`} x1={x} y1={padT} x2={x} y2={padT + chartH} stroke={PAL.grid} />) }
  rows.forEach((r, idx) => {
    const cy = padT + rowH * idx + rowH / 2, bw = chartW * (r.value / maxv), y = cy - bh / 2
    els.push(<rect key={`bg${idx}`} x={padL} y={cy - bh * 0.95} width={chartW} height={bh * 1.9} rx={4} fill={PAL.grid} opacity={0.4} />)
    els.push(<rect key={`b${idx}`} x={padL} y={y} width={Math.max(0, bw)} height={bh} rx={3} fill={r.color || PAL.blue} />)
    els.push(<text key={`l${idx}`} x={padL - 8} y={cy + 3} fontSize={9.5} fill={PAL.ink} textAnchor="end">{r.label}</text>)
    els.push(<text key={`v${idx}`} x={padL + bw + 5} y={cy + 3} fontSize={9.5} fill={PAL.deep} textAnchor="start">{fmt(r.value)}</text>)
    if (r.benchmark != null) { const bx = padL + chartW * (r.benchmark / maxv); els.push(<line key={`bm${idx}`} x1={bx} y1={cy - bh * 1.05} x2={bx} y2={cy + bh * 1.05} stroke={PAL.txt} strokeWidth={1.4} strokeDasharray="2 2" />) }
    if (r.target != null) { const tx = padL + chartW * (r.target / maxv); els.push(<line key={`t${idx}`} x1={tx} y1={cy - bh * 1.15} x2={tx} y2={cy + bh * 1.15} stroke={PAL.ink} strokeWidth={2} />) }
    const endX = padL + bw
    const lines: TipLine[] = [{ label: "Value", val: fmt(r.value), color: r.color || PAL.deep }]
    if (r.target != null) lines.push({ label: "Target", val: fmt(r.target) })
    if (r.benchmark != null) lines.push({ label: "Nat'l median", val: fmt(r.benchmark) })
    const foot = r.target != null ? `${signed(r.value - r.target, fmt)} vs target` : undefined
    zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: endX, cy: cy - bh, node: <TipBody title={r.label} lines={lines} foot={foot} /> })
  })
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

export interface BarRow { label: string; value: number; color?: string }

/** Horizontal bars. */
export function BarH({ rows, fmt, forcedMax, h = H }: { rows: BarRow[]; fmt: (v: number) => string; forcedMax?: number; h?: number }) {
  const padL = 180, padR = 54, padT = 10, padB = 16
  const chartH = h - padT - padB, chartW = W - padL - padR
  let maxv = forcedMax || 0
  if (!forcedMax) { rows.forEach((r) => { if (r.value > maxv) maxv = r.value }); maxv = maxv * 1.14 || 1 }
  const maxRaw = Math.max(...rows.map((r) => r.value), 0) || 1
  const rowH = chartH / Math.max(1, rows.length), bh = Math.min(24, rowH * 0.56)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  for (let i = 0; i <= 4; i++) { const x = padL + chartW * i / 4; els.push(<line key={`g${i}`} x1={x} y1={padT} x2={x} y2={padT + chartH} stroke={PAL.grid} />) }
  rows.forEach((r, idx) => {
    const cy = padT + rowH * idx + rowH / 2, bw = chartW * (r.value / maxv), y = cy - bh / 2
    els.push(<rect key={`b${idx}`} x={padL} y={y} width={Math.max(0, bw)} height={bh} rx={4} fill={r.color || PAL.blue} />)
    els.push(<text key={`l${idx}`} x={padL - 8} y={cy + 3} fontSize={9.5} fill={PAL.ink} textAnchor="end">{r.label}</text>)
    els.push(<text key={`v${idx}`} x={padL + bw + 5} y={cy + 3} fontSize={9.5} fill={PAL.deep} textAnchor="start">{fmt(r.value)}</text>)
    const endX = padL + bw
    zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: endX, cy: cy - bh / 2, node: <TipBody title={r.label} lines={[{ label: "Value", val: fmt(r.value), color: r.color || PAL.deep }]} foot={`${Math.round(r.value / maxRaw * 100)}% of largest`} /> })
  })
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

/** Vertical bars + optional reference line. */
export function BarV({ rows, fmt, forcedMax, refLine, h = H }: { rows: BarRow[]; fmt: (v: number) => string; forcedMax?: number; refLine?: { value: number; label: string }; h?: number }) {
  const padL = 42, padR = 12, padT = 16, padB = 30
  const chartH = h - padT - padB
  let maxv = forcedMax || 0
  if (!forcedMax) { rows.forEach((r) => { if (r.value > maxv) maxv = r.value }); maxv = maxv * 1.14 || 1 }
  const cw = (W - padL - padR) / Math.max(1, rows.length), bw = Math.min(52, cw * 0.6)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  for (let i = 0; i <= 4; i++) { const y = padT + chartH * i / 4, val = maxv * (1 - i / 4); els.push(<line key={`g${i}`} x1={padL} y1={y} x2={W - padR} y2={y} stroke={PAL.grid} />); els.push(<text key={`gt${i}`} x={padL - 6} y={y + 3} fontSize={9} fill={PAL.txt} textAnchor="end">{fmt(val)}</text>) }
  rows.forEach((r, idx) => {
    const cx = padL + cw * idx + cw / 2, x = cx - bw / 2, bhh = chartH * (r.value / maxv), y = padT + chartH - bhh
    els.push(<rect key={`b${idx}`} x={x} y={y} width={bw} height={Math.max(0, bhh)} rx={4} fill={r.color || PAL.blue} />)
    els.push(<text key={`v${idx}`} x={cx} y={y - 4} fontSize={9.5} fill={PAL.ink} textAnchor="middle">{fmt(r.value)}</text>)
    els.push(<text key={`l${idx}`} x={cx} y={h - padB + 14} fontSize={9} fill={PAL.txt} textAnchor="middle">{r.label}</text>)
    const lines: TipLine[] = [{ label: "Value", val: fmt(r.value), color: r.color || PAL.blue }]
    const foot = refLine ? `${signed(r.value - refLine.value, fmt)} vs house avg` : undefined
    zones.push({ x0: cx - cw / 2, x1: cx + cw / 2, y0: padT, y1: padT + chartH, cx, cy: y, node: <TipBody title={r.label} lines={lines} foot={foot} /> })
  })
  if (refLine) { const ry = padT + chartH * (1 - refLine.value / maxv); els.push(<line key="ref" x1={padL} y1={ry} x2={W - padR} y2={ry} stroke={PAL.ink} strokeWidth={1.4} strokeDasharray="4 3" />); els.push(<text key="reft" x={W - padR} y={ry - 3} fontSize={8.5} fill={PAL.ink} textAnchor="end">{refLine.label}</text>) }
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

export interface SparkItem { name: string; color?: string; data: number[]; labels?: string[]; fmt?: (v: number) => string; invert?: boolean }

/** Small-multiples sparklines (2-col grid) with per-point (per-quarter) hover. */
export function SparkGrid({ items, h = H }: { items: SparkItem[]; h?: number }) {
  const cols = 2
  const cw = W / cols, ch = h / Math.ceil(items.length / cols)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  items.forEach((it, idx) => {
    const c = idx % cols, r = Math.floor(idx / cols), ox = c * cw, oy = r * ch
    const padL = 8, padR = 12, padT = 18, padB = 10, sw = cw - padL - padR, sh = ch - padT - padB
    const d = it.data.length ? it.data : [0]
    const labels = it.labels
    let mn = Math.min(...d), mx = Math.max(...d); if (mx === mn) mx = mn + 1
    const X = (i: number) => ox + padL + sw * (d.length <= 1 ? 0 : i / (d.length - 1))
    const Y = (val: number) => oy + padT + sh * (1 - (val - mn) / (mx - mn))
    const f = it.fmt || ((val: number) => `${val}`)
    els.push(<text key={`n${idx}`} x={ox + padL} y={oy + 12} fontSize={9.5} fill={PAL.ink} fontWeight={600}>{it.name}</text>)
    const pts = d.map((val, i) => `${X(i)},${Y(val)}`).join(" ")
    els.push(<polyline key={`p${idx}`} fill="none" stroke={it.color || PAL.deep} strokeWidth={2} points={pts} />)
    // markers at every point so each quarter is visibly hoverable
    d.forEach((val, i) => els.push(<circle key={`pt${idx}-${i}`} cx={X(i)} cy={Y(val)} r={i === d.length - 1 ? 2.6 : 1.8} fill={it.color || PAL.deep} opacity={i === d.length - 1 ? 1 : 0.5} />))
    const delta = Math.round((d[d.length - 1] - d[0]) * 10) / 10
    const da = delta > 0 ? "\u25B2" : (delta < 0 ? "\u25BC" : "\u25AC")
    const good = it.invert ? delta < 0 : delta > 0
    const dcol = delta === 0 ? PAL.txt : (good ? PAL.good : PAL.bad)
    els.push(<text key={`d${idx}`} x={ox + cw - padR} y={oy + 12} fontSize={9} fill={dcol} textAnchor="end">{`${f(d[d.length - 1])} ${da}${Math.abs(delta)}`}</text>)
    // one hit zone per data point (quarter): tooltip shows that specific point's value
    d.forEach((val, i) => {
      const xm = X(i)
      const left = i === 0 ? ox : (X(i - 1) + xm) / 2
      const right = i === d.length - 1 ? ox + cw : (xm + X(i + 1)) / 2
      const prev = i > 0 ? d[i - 1] : null
      const foot = prev != null ? `${signed(Math.round((val - prev) * 10) / 10, f)} vs prev quarter` : (labels ? "first quarter shown" : undefined)
      const title = labels && labels[i] ? labels[i] : `Point ${i + 1} of ${d.length}`
      zones.push({ x0: left, x1: right, y0: oy, y1: oy + ch, cx: xm, cy: Y(val), flip: xm / W > 0.62, node: <TipBody title={title} lines={[{ label: it.name, val: f(val), color: it.color || PAL.deep }]} foot={foot} /> })
    })
  })
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

export interface WaterfallStep { label: string; value: number }

/** Waterfall: start composite -> signed steps -> end (zoomed y). */
export function Waterfall({ startV, steps, endV, fmt, h = H }: { startV: number; steps: WaterfallStep[]; endV: number; fmt: (v: number) => string; h?: number }) {
  const padL = 40, padR = 12, padT = 18, padB = 30
  const chartH = h - padT - padB, chartW = W - padL - padR
  const labels = ["Start", ...steps.map((s) => s.label), "Now"]
  let cum = startV; const allv = [startV]; steps.forEach((s) => { cum += s.value; allv.push(cum) }); allv.push(endV)
  const lo = Math.min(...allv), hi = Math.max(...allv)
  let yMin = Math.floor(lo - 0.6), yMax = Math.ceil(hi + 0.4); if (yMax <= yMin) yMax = yMin + 2
  const Y = (val: number) => padT + chartH * (1 - (val - yMin) / (yMax - yMin))
  const n = labels.length, cw = chartW / n, bw = Math.min(44, cw * 0.56)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  for (let i = 0; i <= 4; i++) { const yy = padT + chartH * i / 4, val = yMax - (yMax - yMin) * i / 4; els.push(<line key={`g${i}`} x1={padL} y1={yy} x2={W - padR} y2={yy} stroke={PAL.grid} />); els.push(<text key={`gt${i}`} x={padL - 6} y={yy + 3} fontSize={9} fill={PAL.txt} textAnchor="end">{fmt(val)}</text>) }
  const cumAfter = [startV]; let run0 = startV; steps.forEach((s) => { run0 += s.value; cumAfter.push(run0) }); cumAfter.push(endV)
  let run = startV
  for (let k = 0; k < n; k++) {
    const cx = padL + cw * k + cw / 2, x = cx - bw / 2
    let top: number, bot: number, col: string, vlabel: string
    let title: string; const lines: TipLine[] = []; let foot: string | undefined
    if (k === 0) { top = Y(startV); bot = Y(yMin); col = PAL.deep; vlabel = fmt(startV); title = "Start"; lines.push({ label: "Composite", val: fmt(startV) }) }
    else if (k === n - 1) { top = Y(endV); bot = Y(yMin); col = PAL.deep; vlabel = fmt(endV); title = "Now"; lines.push({ label: "Composite", val: fmt(endV) }) }
    else { const sv = steps[k - 1].value, before = run, after = run + sv; run = after; top = Y(Math.max(before, after)); bot = Y(Math.min(before, after)); col = sv < 0 ? PAL.bad : PAL.good; vlabel = `${sv < 0 ? "" : "+"}${Math.round(sv * 10) / 10}`; title = steps[k - 1].label; lines.push({ label: "Contribution", val: signed(sv, fmt), color: col }); foot = `Composite after : ${fmt(cumAfter[k])}` }
    els.push(<rect key={`b${k}`} x={x} y={top} width={bw} height={Math.max(1, bot - top)} rx={3} fill={col} />)
    els.push(<text key={`v${k}`} x={cx} y={top - 4} fontSize={9} fill={PAL.ink} textAnchor="middle">{vlabel}</text>)
    els.push(<text key={`l${k}`} x={cx} y={h - padB + 13} fontSize={8} fill={PAL.txt} textAnchor="middle">{labels[k]}</text>)
    if (k < n - 1) { const yc = Y(cumAfter[k]); els.push(<line key={`c${k}`} x1={cx + bw / 2} y1={yc} x2={cx + cw - bw / 2} y2={yc} stroke={PAL.txt} strokeDasharray="3 3" strokeWidth={1} />) }
    zones.push({ x0: cx - cw / 2, x1: cx + cw / 2, y0: padT, y1: padT + chartH, cx, cy: top, node: <TipBody title={title} lines={lines} foot={foot} /> })
  }
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

export interface FunnelStep { label: string; value: number; target?: number }

/** Centered funnel with target edge-ticks. */
export function FunnelH({ steps, fmt, h = H }: { steps: FunnelStep[]; fmt: (v: number) => string; h?: number }) {
  const padL = 6, padR = 6, padT = 12, padB = 8
  const chartH = h - padT - padB, chartW = W - padL - padR
  let maxv = 100; steps.forEach((s) => { if (s.value > maxv) maxv = s.value })
  const rowH = chartH / Math.max(1, steps.length), bh = Math.min(30, rowH * 0.54), cxm = padL + chartW / 2
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  steps.forEach((s, idx) => {
    const cy = padT + rowH * idx + rowH / 2, bw = chartW * (s.value / maxv), x = cxm - bw / 2, y = cy - bh / 2
    const col = s.value >= (s.target || 0) ? PAL.good : (s.value >= (s.target || 0) * 0.85 ? PAL.warn : PAL.bad)
    els.push(<text key={`l${idx}`} x={cxm} y={y - 3} fontSize={9} fill={PAL.ink} textAnchor="middle">{s.label}</text>)
    els.push(<rect key={`b${idx}`} x={x} y={y} width={Math.max(2, bw)} height={bh} rx={4} fill={col} />)
    els.push(<text key={`v${idx}`} x={cxm} y={cy + 3} fontSize={10} fill="#fff" textAnchor="middle" fontWeight={700}>{fmt(s.value)}</text>)
    if (s.target != null) { const tw = chartW * (s.target / maxv); els.push(<line key={`t1${idx}`} x1={cxm - tw / 2} y1={y - 2} x2={cxm - tw / 2} y2={y + bh + 2} stroke={PAL.ink} strokeWidth={1.5} />); els.push(<line key={`t2${idx}`} x1={cxm + tw / 2} y1={y - 2} x2={cxm + tw / 2} y2={y + bh + 2} stroke={PAL.ink} strokeWidth={1.5} />) }
    const lines: TipLine[] = [{ label: "Rate", val: fmt(s.value), color: col }]
    if (s.target != null) lines.push({ label: "Target", val: fmt(s.target) })
    const foot = s.target != null ? `${signed(s.value - s.target, fmt)} vs target` : undefined
    zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: cxm, cy: y, flip: false, node: <TipBody title={s.label} lines={lines} foot={foot} /> })
  })
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

/** Diverging bar: signed values centered on zero. */
export function DivBar({ rows, fmt, h = 190 }: { rows: BarRow[]; fmt: (v: number) => string; h?: number }) {
  const padL = 150, padR = 30, padT = 10, padB = 16
  const chartH = h - padT - padB, chartW = W - padL - padR, cx0 = padL + chartW / 2
  let maxAbs = 0; rows.forEach((r) => { if (Math.abs(r.value) > maxAbs) maxAbs = Math.abs(r.value) }); maxAbs = maxAbs * 1.2 || 1
  const rowH = chartH / Math.max(1, rows.length), bh = Math.min(16, rowH * 0.5)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  els.push(<line key="axis" x1={cx0} y1={padT} x2={cx0} y2={padT + chartH} stroke={PAL.ink} strokeWidth={1} />)
  rows.forEach((r, idx) => {
    const cy = padT + rowH * idx + rowH / 2, bw = (chartW / 2) * (Math.abs(r.value) / maxAbs), y = cy - bh / 2
    const col = r.value >= 0 ? PAL.good : (r.value <= -4 ? PAL.bad : PAL.warn), x = r.value >= 0 ? cx0 : cx0 - bw
    els.push(<rect key={`b${idx}`} x={x} y={y} width={Math.max(1, bw)} height={bh} rx={3} fill={col} />)
    els.push(<text key={`l${idx}`} x={padL - 8} y={cy + 3} fontSize={9.5} fill={PAL.ink} textAnchor="end">{r.label}</text>)
    const lx = r.value >= 0 ? x + bw + 4 : x - 4, anc = r.value >= 0 ? "start" : "end"
    els.push(<text key={`v${idx}`} x={lx} y={cy + 3} fontSize={9} fill={PAL.deep} textAnchor={anc as "start" | "end"}>{`${r.value > 0 ? "+" : ""}${fmt(r.value)}`}</text>)
    const anchorX = r.value >= 0 ? x + bw : x
    zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: anchorX, cy: cy - bh / 2, node: <TipBody title={r.label} lines={[{ label: "vs house avg", val: signed(r.value, fmt), color: col }]} foot={r.value >= 0 ? "above average" : "below average"} /> })
  })
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

/** Diverging deviation-from-house-average bars, sorted, centered on the reference. */
export function DeviationBar({ rows, houseAvg, fmt, h = H }: { rows: BarRow[]; houseAvg: number; fmt: (v: number) => string; h?: number }) {
  const padL = 104, padR = 40, padT = 20, padB = 8
  const chartH = h - padT - padB, chartW = W - padL - padR, cx0 = padL + chartW / 2
  const pts = (d: number) => `${d > 0 ? "+" : d < 0 ? "\u2212" : ""}${Math.abs(Math.round(d * 10) / 10)} pts`
  const devs = rows.map((r) => ({ label: r.label, value: r.value, dev: Math.round((r.value - houseAvg) * 10) / 10 })).sort((a, b) => a.dev - b.dev)
  let maxAbs = 0; devs.forEach((d) => { if (Math.abs(d.dev) > maxAbs) maxAbs = Math.abs(d.dev) }); maxAbs = maxAbs * 1.18 || 1
  const rowH = chartH / Math.max(1, devs.length), bh = Math.min(14, rowH * 0.5)
  const X = (d: number) => cx0 + (chartW / 2) * (d / maxAbs)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  els.push(<line key="axis" x1={cx0} y1={padT - 6} x2={cx0} y2={padT + chartH} stroke={PAL.ink} strokeWidth={1} />)
  els.push(<text key="ha" x={cx0} y={padT - 9} fontSize={8.5} fill={PAL.ink} textAnchor="middle">House avg {fmt(houseAvg)}</text>)
  devs.forEach((d, idx) => {
    const cy = padT + rowH * idx + rowH / 2, x = X(d.dev)
    const col = d.dev >= 0 ? PAL.good : (d.dev <= -8 ? PAL.bad : PAL.warn)
    const bx = d.dev >= 0 ? cx0 : x, bw = Math.abs(x - cx0)
    els.push(<rect key={`b${idx}`} x={bx} y={cy - bh / 2} width={Math.max(1, bw)} height={bh} rx={3} fill={col} />)
    els.push(<text key={`l${idx}`} x={padL - 6} y={cy + 3} fontSize={9} fill={PAL.ink} textAnchor="end">{d.label}</text>)
    // adaptive value placement: inside the bar (white) when it is long enough, else just outside the tip (colored)
    const inside = bw >= 46
    let lx: number, anc: "start" | "end", fill: string
    if (d.dev >= 0) { if (inside) { lx = x - 4; anc = "end"; fill = "#fff" } else { lx = x + 4; anc = "start"; fill = PAL.deep } }
    else { if (inside) { lx = x + 4; anc = "start"; fill = "#fff" } else { lx = x - 4; anc = "end"; fill = PAL.deep } }
    els.push(<text key={`v${idx}`} x={lx} y={cy + 3} fontSize={8.5} fill={fill} textAnchor={anc}>{pts(d.dev)}</text>)
    zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: x, cy: cy - bh / 2, node: <TipBody title={d.label} lines={[{ label: "Composite", val: fmt(d.value), color: col }]} foot={`${pts(d.dev)} vs house avg (${fmt(houseAvg)})`} /> })
  })
  return <ChartFrame h={h} zones={zones}>{els}</ChartFrame>
}

/** Horizontal waterfall bridge (top->bottom); near-zero steps collapsed into an "Other" bucket. */
export function HWaterfall({ startV, steps, endV, fmt, h = H, collapseBelow = 0.05 }: { startV: number; steps: WaterfallStep[]; endV: number; fmt: (v: number) => string; h?: number; collapseBelow?: number }) {
  const big = steps.filter((s) => Math.abs(s.value) >= collapseBelow)
  const otherSum = Math.round(steps.filter((s) => Math.abs(s.value) < collapseBelow).reduce((a, s) => a + s.value, 0) * 100) / 100
  const arr: WaterfallStep[] = big.length < steps.length ? big.concat([{ label: "Other", value: otherSum }]) : big.slice()
  type WRow = { n: string; kind: "anchor" | "step"; end?: number; before?: number; after?: number; v?: number }
  const rows: WRow[] = [{ n: "Start", kind: "anchor", end: startV }]
  let cum = startV
  arr.forEach((s) => { const before = cum; cum = Math.round((cum + s.value) * 100) / 100; rows.push({ n: s.label, kind: "step", before, after: cum, v: s.value }) })
  rows.push({ n: "Now", kind: "anchor", end: endV })
  const allv = [startV, endV]; { let c = startV; arr.forEach((s) => { c += s.value; allv.push(Math.round(c * 100) / 100) }) }
  let lo = Math.min(...allv), hi = Math.max(...allv); const pad = Math.max(0.2, (hi - lo) * 0.28); lo -= pad; hi += pad
  const padL = 118, padR = 46, padT = 16, padB = 8
  const rowH = Math.min(24, (h - padT - padB) / Math.max(1, rows.length))
  const usedH = padT + rows.length * rowH + padB, chartW = W - padL - padR
  const X = (v: number) => padL + chartW * (v - lo) / (hi - lo)
  const els: React.ReactNode[] = []
  const zones: Zone[] = []
  for (let g = Math.ceil(lo); g <= Math.floor(hi); g++) { els.push(<line key={`g${g}`} x1={X(g)} y1={padT} x2={X(g)} y2={padT + rows.length * rowH} stroke={PAL.grid} />); els.push(<text key={`gt${g}`} x={X(g)} y={padT - 3} fontSize={8} fill={PAL.txt} textAnchor="middle">{g}%</text>) }
  rows.forEach((r, i) => {
    const cy = padT + rowH * i + rowH / 2
    els.push(<text key={`l${i}`} x={padL - 6} y={cy + 3} fontSize={8.5} fill={PAL.ink} textAnchor="end">{r.n}</text>)
    if (r.kind === "anchor") {
      const bw = X(r.end!) - X(lo)
      els.push(<rect key={`b${i}`} x={padL} y={cy - 7} width={Math.max(0, bw)} height={14} rx={3} fill={PAL.deep} />)
      els.push(<text key={`v${i}`} x={padL + bw + 5} y={cy + 3} fontSize={9} fill={PAL.deep} fontWeight={700} textAnchor="start">{fmt(r.end!)}</text>)
      zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: padL + bw, cy: cy - 7, node: <TipBody title={r.n} lines={[{ label: "Composite", val: fmt(r.end!) }]} /> })
    } else {
      const xa = X(Math.min(r.before!, r.after!)), xb = X(Math.max(r.before!, r.after!)), col = r.v! < 0 ? PAL.bad : (r.v! > 0 ? PAL.good : PAL.txt)
      els.push(<rect key={`b${i}`} x={xa} y={cy - 6} width={Math.max(2, xb - xa)} height={12} rx={2} fill={col} />)
      const dl = `${r.v! < 0 ? "\u2212" : r.v! > 0 ? "+" : ""}${Math.abs(Math.round(r.v! * 10) / 10)}`
      els.push(<text key={`v${i}`} x={xb + 5} y={cy + 3} fontSize={8.5} fill={col} textAnchor="start">{dl}</text>)
      zones.push({ x0: 0, x1: W, y0: cy - rowH / 2, y1: cy + rowH / 2, cx: xb, cy: cy - 6, node: <TipBody title={r.n} lines={[{ label: "Contribution", val: `${dl} pts`, color: col }]} foot={`Composite after : ${fmt(r.after!)}`} /> })
    }
  })
  return <ChartFrame h={usedH} zones={zones}>{els}</ChartFrame>
}
