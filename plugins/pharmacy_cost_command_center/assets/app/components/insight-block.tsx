"use client"

import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { useInsights } from "@/lib/use-data"

/** Renders the pre-baked AI Powered Insights row for a band (from MDL_TAB_INSIGHTS). */
export function InsightBlock({ bandKey, onAsk }: { bandKey: string; onAsk: () => void }) {
  const { data, isLoading, error } = useInsights()
  const insight = data?.insights.find((i) => i.tabKey === bandKey)

  return (
    <div className="insights">
      <div className="ihead">
        <span className="aibadge">
          <span className="spark2" />
          AI-powered
        </span>
        <b>AI Powered Insights</b>
      </div>
      <div className="agentmsg">
        {isLoading ? (
          <div className="ambody">Loading insights…</div>
        ) : error ? (
          <div className="ambody" style={{ color: "var(--bad)" }}>
            {error instanceof Error ? error.message : "Failed to load insights"}
          </div>
        ) : insight ? (
          <>
            <div className="amhead">
              <span className="amdot" />
              {insight.persona}
            </div>
            <div className="ambody">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>{insight.md}</ReactMarkdown>
            </div>
            <div className="itags">
              {insight.tags.map((t) => (
                <span key={t} className={"tagi" + (t.toUpperCase() === "POLICY" ? " policy" : "")}>
                  {t.toUpperCase()}
                </span>
              ))}
              <button className="tagi" style={{ cursor: "pointer", marginLeft: "auto" }} onClick={onAsk}>
                Ask the agent →
              </button>
            </div>
          </>
        ) : (
          <div className="ambody">No insight available for this band.</div>
        )}
      </div>
    </div>
  )
}
