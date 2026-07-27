import { querySnowflake } from "@/lib/snowflake"
import { PANEL_SQL, isPanelKey } from "@/lib/queries"

export const dynamic = "force-dynamic"

/**
 * GET /api/panel/[key]
 * Returns raw rows for a whitelisted panel. `key` is validated against the
 * PANEL_SQL map — nothing from the URL is ever interpolated into SQL.
 */
export async function GET(
  _req: Request,
  { params }: { params: Promise<{ key: string }> },
) {
  const { key } = await params
  if (!isPanelKey(key)) {
    return Response.json({ error: `Unknown panel: ${key}` }, { status: 404 })
  }
  try {
    const rows = await querySnowflake(PANEL_SQL[key])
    return Response.json({ rows })
  } catch (e) {
    console.error(new Date().toISOString(), `[api/panel/${key}] query failed`, e)
    return Response.json(
      { error: e instanceof Error ? e.message : "Query failed" },
      { status: 500 },
    )
  }
}
