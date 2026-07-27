"use client"

import type { ReactNode } from "react"
import { usePanel } from "@/lib/use-data"

interface PanelCardProps {
  title: string
  hint?: ReactNode
  panelKey: string
  span2?: boolean
  height?: number
  children: (rows: Record<string, any>[]) => ReactNode
}

/** A dashboard card that fetches its whitelisted panel's LIVE rows and renders a chart. */
export function PanelCard({ title, hint, panelKey, span2, height = 240, children }: PanelCardProps) {
  const { data, isLoading, error } = usePanel(panelKey)
  return (
    <div className={"card" + (span2 ? " span2" : "")}>
      <h3>{title}</h3>
      {hint ? <p className="hint">{hint}</p> : null}
      <div className="chartbox" style={{ height }}>
        {isLoading ? (
          <div className="loadingbox">Loading live data…</div>
        ) : error ? (
          <div className="errorbox">{error instanceof Error ? error.message : "Query failed"}</div>
        ) : data && data.rows.length > 0 ? (
          children(data.rows)
        ) : (
          <div className="loadingbox">No data returned.</div>
        )}
      </div>
    </div>
  )
}
