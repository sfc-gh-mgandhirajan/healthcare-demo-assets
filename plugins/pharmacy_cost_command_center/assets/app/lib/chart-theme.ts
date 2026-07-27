/** Recharts palette — mirrors the reference mockup's PAL. */
export const CHART = {
  blue: "#29B5E8",
  deep: "#1D5F8A",
  navy: "#11567F",
  good: "#2E9E6B",
  warn: "#d98b1a",
  bad: "#e5484d",
  purple: "#7b5bd6",
  orange: "#e0863d",
  grid: "#e6edf3",
  txt: "#5b6b7a",
  ink: "#11242f",
} as const

/** Rank-ordered categorical colors for multi-series charts. */
export const SERIES = [
  CHART.bad,
  CHART.warn,
  CHART.blue,
  CHART.deep,
  CHART.purple,
  CHART.orange,
  CHART.good,
  CHART.navy,
]
