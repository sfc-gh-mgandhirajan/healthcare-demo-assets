"use client"

import { useKpi } from "@/lib/use-data"
import { dollars2, moneyM, signedPct1, pct1, moneyFull } from "@/lib/format"

export function KpiRibbon() {
  const { data, isLoading, error } = useKpi()

  if (isLoading) {
    return (
      <div className="ribbon">
        {[0, 1, 2, 3].map((i) => (
          <div className="kpi" key={i}>
            <div className="lab">Loading…</div>
            <div className="val">—</div>
          </div>
        ))}
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="ribbon">
        <div className="kpi" style={{ gridColumn: "1 / -1" }}>
          <div className="lab">KPI summary</div>
          <div className="delta up">{error instanceof Error ? error.message : "Failed to load"}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="ribbon">
      <div className="kpi">
        <div className="lab">Net Rx PMPM</div>
        <div className="val">{dollars2(data.netPmpmCurrent)}</div>
        <div className="delta up">▲ {signedPct1(data.pmpmYoyPct)} YoY</div>
      </div>
      <div className="kpi warnborder">
        <div className="lab">PMPM vs plan</div>
        <div className="val" style={{ color: "var(--bad)" }}>
          {data.pmpmVsPlan != null && data.pmpmVsPlan > 0 ? "+" : ""}
          {dollars2(data.pmpmVsPlan)}
        </div>
        <div className="delta up">▲ over plan ({dollars2(data.targetPmpm)})</div>
      </div>
      <div className="kpi">
        <div className="lab">Projected next 12mo</div>
        <div className="val" title={moneyFull(data.projectedNext12Spend)}>{moneyM(data.projectedNext12Spend)}</div>
        <div className="delta flat">forecast · base</div>
      </div>
      <div className="kpi warnborder">
        <div className="lab">Addressable now</div>
        <div className="val" style={{ color: "var(--bad)" }} title={moneyFull(data.addressableTotal)}>
          {moneyM(data.addressableTotal)}
        </div>
        <div className="delta flat">{pct1(data.addressablePctOfSpend)} of spend</div>
      </div>
    </div>
  )
}
