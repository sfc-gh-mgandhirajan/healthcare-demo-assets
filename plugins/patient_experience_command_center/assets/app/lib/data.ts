/**
 * Server-side data access for the Patient Experience Command Center.
 * Reads ONLY the certified VW_* serving views (never the underlying MDL_/DENORM_/DT_ objects)
 * and the governed action queue. All queries run under owner's rights.
 */
import { querySnowflake } from "@/lib/snowflake"
import { PX_FQN } from "@/lib/constants"

export type Row = Record<string, any>

export interface HouseKpis {
  COMPOSITE_TOPBOX: number
  STAR_LEVEL: number
  PTS_TO_NEXT_STAR: number
  RECOMMEND_TOPBOX_PCT: number
  HIGH_RISK_ENCOUNTERS: number
  AVG_DETRACTOR_RISK: number
  NET_INPATIENT_REVENUE: number
  VBP_AT_RISK_USD: number
  COMPOSITE_YOY_DELTA_PTS: number
  PATIENTS_AWAITING_RECOVERY: number
  FORMAL_GRIEVANCES_OPEN: number
}

export interface CommandCenterData {
  kpis: HouseKpis | null
  journey: Row[]
  waitByArea: Row[]
  accessMomentum: Row[]
  measureBenchmark: Row[]
  measureTrend: Row[]
  equityGap: Row[]
  driverImportance: Row[]
  unitScorecard: Row[]
  pointsLost: Row[]
  dischargeReadiness: Row[]
  grievanceAging: Row[]
  recoverySla: Row[]
  voiceThemes: Row[]
  tabInsights: Row[]
  errors: string[]
}

async function safe(label: string, sql: string, errors: string[]): Promise<Row[]> {
  try {
    return await querySnowflake(sql)
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    console.error(new Date().toISOString(), `[data] ${label} failed:`, msg)
    errors.push(`${label}: ${msg}`)
    return []
  }
}

const v = (name: string) => `${PX_FQN}.${name}`

export async function getCommandCenterData(): Promise<CommandCenterData> {
  const errors: string[] = []

  const [
    kpisRows,
    journey,
    waitByArea,
    accessMomentum,
    measureBenchmark,
    measureTrend,
    equityGap,
    driverImportance,
    unitScorecard,
    pointsLost,
    dischargeReadiness,
    grievanceAging,
    recoverySla,
    voiceThemes,
    tabInsights,
  ] = await Promise.all([
    safe("VW_HOUSE_KPIS", `SELECT * FROM ${v("VW_HOUSE_KPIS")}`, errors),
    safe("VW_JOURNEY_RIBBON", `SELECT * FROM ${v("VW_JOURNEY_RIBBON")} ORDER BY STAGE_ORDER`, errors),
    safe("VW_WAIT_BY_AREA", `SELECT * FROM ${v("VW_WAIT_BY_AREA")} ORDER BY AVG_WAIT_MIN DESC`, errors),
    safe("VW_ACCESS_MOMENTUM", `SELECT * FROM ${v("VW_ACCESS_MOMENTUM")} ORDER BY QUARTER`, errors),
    safe("VW_MEASURE_BENCHMARK", `SELECT * FROM ${v("VW_MEASURE_BENCHMARK")}`, errors),
    safe("VW_MEASURE_TREND", `SELECT * FROM ${v("VW_MEASURE_TREND")} ORDER BY QUARTER, MEASURE_CODE`, errors),
    safe("VW_EQUITY_GAP", `SELECT * FROM ${v("VW_EQUITY_GAP")} ORDER BY GAP_PTS`, errors),
    safe("VW_DRIVER_IMPORTANCE", `SELECT * FROM ${v("VW_DRIVER_IMPORTANCE")} ORDER BY RANK`, errors),
    safe("VW_UNIT_SCORECARD", `SELECT * FROM ${v("VW_UNIT_SCORECARD")} ORDER BY COMPOSITE_TOPBOX`, errors),
    safe("VW_POINTS_LOST", `SELECT * FROM ${v("VW_POINTS_LOST")}`, errors),
    safe("VW_DISCHARGE_READINESS", `SELECT * FROM ${v("VW_DISCHARGE_READINESS")} ORDER BY STEP_ORDER`, errors),
    safe("VW_GRIEVANCE_AGING", `SELECT * FROM ${v("VW_GRIEVANCE_AGING")} ORDER BY BUCKET_ORDER`, errors),
    safe("VW_RECOVERY_SLA", `SELECT * FROM ${v("VW_RECOVERY_SLA")} ORDER BY SLA_ORDER`, errors),
    safe("VW_VOICE_THEMES", `SELECT * FROM ${v("VW_VOICE_THEMES")}`, errors),
    safe("VW_TAB_INSIGHTS", `SELECT * FROM ${v("VW_TAB_INSIGHTS")} ORDER BY STAGE_ORDER`, errors),
  ])

  return {
    kpis: (kpisRows[0] as HouseKpis) ?? null,
    journey,
    waitByArea,
    accessMomentum,
    measureBenchmark,
    measureTrend,
    equityGap,
    driverImportance,
    unitScorecard,
    pointsLost,
    dischargeReadiness,
    grievanceAging,
    recoverySla,
    voiceThemes,
    tabInsights,
    errors,
  }
}
