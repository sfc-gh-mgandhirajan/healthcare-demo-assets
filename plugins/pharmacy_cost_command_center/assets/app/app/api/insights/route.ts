import { querySnowflake } from "@/lib/snowflake"
import { INSIGHTS_SQL } from "@/lib/queries"

export const dynamic = "force-dynamic"

/** Snowflake ARRAY columns arrive as a JS array or a JSON string; normalize. */
function toTags(raw: unknown): string[] {
  if (Array.isArray(raw)) return raw.map(String)
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) return parsed.map(String)
    } catch {
      return raw.split(",").map((s) => s.trim()).filter(Boolean)
    }
  }
  return []
}

export async function GET() {
  try {
    const rows = await querySnowflake(INSIGHTS_SQL)
    const insights = rows.map((r) => ({
      tabOrder: Number(r.TAB_ORDER),
      tabKey: String(r.TAB_KEY),
      tabTitle: String(r.TAB_TITLE),
      persona: String(r.AGENT_PERSONA),
      md: String(r.INSIGHT_MD ?? ""),
      tags: toTags(r.TAGS),
      generatedAt: r.GENERATED_AT ? String(r.GENERATED_AT) : null,
    }))
    return Response.json({ insights })
  } catch (e) {
    console.error(new Date().toISOString(), "[api/insights] query failed", e)
    return Response.json(
      { error: e instanceof Error ? e.message : "Failed to load insights" },
      { status: 500 },
    )
  }
}
