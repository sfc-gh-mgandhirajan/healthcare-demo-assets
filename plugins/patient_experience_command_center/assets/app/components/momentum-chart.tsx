"use client"
import React, { useMemo, useRef, useState } from "react"
import type { Row } from "@/lib/data"
import { PAL } from "@/components/charts"

const num = (v: unknown) => { const n = Number(v); return Number.isFinite(n) ? n : 0 }

function fmtQuarter(q: unknown): string {
  const d = new Date(String(q))
  if (isNaN(d.getTime())) return String(q)
  const qtr = Math.floor(d.getUTCMonth() / 3) + 1
  return `Q${qtr} '${String(d.getUTCFullYear()).slice(2)}`
}

const W = 560, H = 232
const padL = 42, padR = 46, padT = 26, padB = 42
const chartW = W - padL - padR, chartH = H - padT - padB

/**
 * Access experience momentum — on-time access % (left axis) and LWBS rate % (right axis)
 * on a shared quarterly X axis, with a hover guide + value tooltip.
 */
export function MomentumChart({ rows }: { rows: Row[] }) {
  const [hover, setHover] = useState<number | null>(null)
  const wrapRef = useRef<HTMLDivElement>(null)

  const data = useMemo(() => rows.map((r) => ({
    q: fmtQuarter(r.QUARTER), onTime: num(r.ON_TIME_PCT), lwbs: num(r.LWBS_PCT), n: num(r.N_VISITS),
  })), [rows])
  const n = data.length

  const scales = useMemo(() => {
    const onVals = data.map((d) => d.onTime)
    const lwVals = data.map((d) => d.lwbs)
    let loL = Math.floor(Math.min(...onVals, 100) - 3)
    let hiL = Math.ceil(Math.max(...onVals, 0) + 3)
    if (hiL <= loL) hiL = loL + 5
    let hiR = Math.ceil(Math.max(...lwVals, 0) * 1.35 * 10) / 10
    if (hiR <= 0) hiR = 1
    return { loL, hiL, loR: 0, hiR }
  }, [data])

  if (!n) return <div style={{ fontSize: 12, color: "var(--ink-soft)", padding: "24px 0", textAlign: "center" }}>No data available.</div>

  const X = (i: number) => padL + chartW * (n <= 1 ? 0.5 : i / (n - 1))
  const YL = (v: number) => padT + chartH * (1 - (v - scales.loL) / (scales.hiL - scales.loL))
  const YR = (v: number) => padT + chartH * (1 - (v - scales.loR) / (scales.hiR - scales.loR))

  const onPts = data.map((d, i) => `${X(i)},${YL(d.onTime)}`).join(" ")
  const lwPts = data.map((d, i) => `${X(i)},${YR(d.lwbs)}`).join(" ")

  const grid: React.ReactNode[] = []
  for (let i = 0; i <= 4; i++) {
    const y = padT + chartH * i / 4
    const lv = scales.hiL - (scales.hiL - scales.loL) * i / 4
    const rv = scales.hiR - (scales.hiR - scales.loR) * i / 4
    grid.push(<line key={`g${i}`} x1={padL} y1={y} x2={W - padR} y2={y} stroke={PAL.grid} />)
    grid.push(<text key={`gl${i}`} x={padL - 6} y={y + 3} fontSize={9} fill={PAL.deep} textAnchor="end">{Math.round(lv)}%</text>)
    grid.push(<text key={`gr${i}`} x={W - padR + 6} y={y + 3} fontSize={9} fill={PAL.bad} textAnchor="start">{(Math.round(rv * 10) / 10)}%</text>)
  }

  function onMove(e: React.MouseEvent) {
    const rect = wrapRef.current?.getBoundingClientRect(); if (!rect) return
    const frac = (e.clientX - rect.left) / rect.width
    let best = 0, bestD = Infinity
    for (let i = 0; i < n; i++) { const d = Math.abs(X(i) / W - frac); if (d < bestD) { bestD = d; best = i } }
    setHover(best)
  }

  const hi = hover
  const tipLeftPct = hi != null ? (X(hi) / W) * 100 : 0
  const flip = hi != null && hi > n / 2

  return (
    <div ref={wrapRef} style={{ position: "relative" }} onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" preserveAspectRatio="xMidYMid meet" style={{ display: "block", height: "auto" }}>
        {grid}
        {/* legend */}
        <line x1={padL} y1={12} x2={padL + 16} y2={12} stroke={PAL.deep} strokeWidth={2.4} />
        <text x={padL + 20} y={15} fontSize={9.5} fill={PAL.txt}>Access on-time % (L)</text>
        <line x1={padL + 150} y1={12} x2={padL + 166} y2={12} stroke={PAL.bad} strokeWidth={2.4} />
        <text x={padL + 170} y={15} fontSize={9.5} fill={PAL.txt}>LWBS rate % (R)</text>

        {hi != null && <line x1={X(hi)} y1={padT} x2={X(hi)} y2={padT + chartH} stroke={PAL.txt} strokeDasharray="3 3" strokeWidth={1} />}

        <polyline fill="none" stroke={PAL.deep} strokeWidth={2.4} points={onPts} />
        <polyline fill="none" stroke={PAL.bad} strokeWidth={2.4} points={lwPts} />

        {data.map((d, i) => (
          <g key={i}>
            <circle cx={X(i)} cy={YL(d.onTime)} r={hi === i ? 4 : 2.6} fill={PAL.deep} />
            <circle cx={X(i)} cy={YR(d.lwbs)} r={hi === i ? 4 : 2.6} fill={PAL.bad} />
            <text x={X(i)} y={H - padB + 15} fontSize={9} fill={PAL.txt} textAnchor="middle">{d.q}</text>
          </g>
        ))}
      </svg>

      {hi != null && (
        <div style={{
          position: "absolute", top: 34, left: `${tipLeftPct}%`,
          transform: `translateX(${flip ? "-100%" : "0"}) translateX(${flip ? -8 : 8}px)`,
          background: "#fff", border: "1px solid var(--line)", borderRadius: 10,
          boxShadow: "0 8px 22px rgba(17,86,127,.16)", padding: "9px 12px", pointerEvents: "none",
          minWidth: 150, zIndex: 5,
        }}>
          <div style={{ fontSize: 12.5, fontWeight: 800, color: "var(--ink)", marginBottom: 5 }}>{data[hi].q}</div>
          <div style={{ fontSize: 12, color: PAL.deep, marginBottom: 2 }}>Access on-time : <b>{data[hi].onTime.toFixed(1)}%</b></div>
          <div style={{ fontSize: 12, color: PAL.bad }}>LWBS rate : <b>{data[hi].lwbs.toFixed(1)}%</b></div>
          {data[hi].n > 0 && <div style={{ fontSize: 10.5, color: "var(--ink-soft)", marginTop: 4 }}>{data[hi].n.toLocaleString()} visits</div>}
        </div>
      )}
    </div>
  )
}
