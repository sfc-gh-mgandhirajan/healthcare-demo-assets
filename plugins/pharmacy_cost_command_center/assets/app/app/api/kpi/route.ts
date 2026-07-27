import { querySnowflake } from "@/lib/snowflake"
import { KPI_SQL } from "@/lib/queries"
import { num } from "@/lib/format"

export const dynamic = "force-dynamic"

export async function GET() {
  try {
    const rows = await querySnowflake(KPI_SQL)
    const r = rows[0] ?? {}
    return Response.json({
      netPmpmCurrent: num(r.NET_PMPM_CURRENT),
      netPmpmPrior: num(r.NET_PMPM_PRIOR),
      pmpmYoyPct: num(r.PMPM_YOY_PCT),
      targetPmpm: num(r.TARGET_PMPM),
      pmpmVsPlan: num(r.PMPM_VS_PLAN),
      projectedNext12Spend: num(r.PROJECTED_NEXT12_SPEND),
      addressableTotal: num(r.ADDRESSABLE_TOTAL),
      netSpendCurrent12: num(r.NET_SPEND_CURRENT_12),
      addressablePctOfSpend: num(r.ADDRESSABLE_PCT_OF_SPEND),
    })
  } catch (e) {
    console.error(new Date().toISOString(), "[api/kpi] query failed", e)
    return Response.json(
      { error: e instanceof Error ? e.message : "Failed to load KPI summary" },
      { status: 500 },
    )
  }
}
