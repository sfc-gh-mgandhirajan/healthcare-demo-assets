/** Client-safe formatters. All inputs are raw values straight from the views. */

/** Raw dollars -> "$123.8M" / "$21M". */
export function moneyM(dollars: number | null | undefined): string {
  if (dollars == null || Number.isNaN(dollars)) return "—"
  const m = dollars / 1_000_000
  const abs = Math.abs(m)
  const digits = abs >= 100 ? 0 : abs >= 10 ? 1 : 2
  return `$${m.toFixed(digits)}M`
}

/** Raw dollars -> "$123,836,872" (tooltips / precise readouts). */
export function moneyFull(dollars: number | null | undefined): string {
  if (dollars == null || Number.isNaN(dollars)) return "—"
  return `$${Math.round(dollars).toLocaleString("en-US")}`
}

/** PMPM etc. -> "$120.23". */
export function dollars2(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  return `$${v.toFixed(2)}`
}

/** Whole dollars -> "$120". */
export function dollars0(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  return `$${Math.round(v).toLocaleString("en-US")}`
}

/** A value that is already a percentage number (e.g. 33.3) -> "33.3%". */
export function pct1(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  return `${v.toFixed(1)}%`
}

export function pct0(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  return `${Math.round(v)}%`
}

/** A 0–1 fraction (PDC, capture rate) -> "0.31". */
export function dec2(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  return v.toFixed(2)
}

/** Signed percentage with arrow-friendly sign, e.g. "+33.3%" / "−4.0%". */
export function signedPct1(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  const sign = v > 0 ? "+" : v < 0 ? "−" : ""
  return `${sign}${Math.abs(v).toFixed(1)}%`
}

export function intFmt(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—"
  return Math.round(v).toLocaleString("en-US")
}

/** Coerce a Snowflake numeric (may arrive as string) to number | null. */
export function num(v: unknown): number | null {
  if (v == null) return null
  const n = typeof v === "number" ? v : Number(v)
  return Number.isNaN(n) ? null : n
}
