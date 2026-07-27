/**
 * Governed action write path. POST logs an OPEN action to ACTION_EVENT_LOG
 * (owner's rights) so the service-recovery / improvement lifecycle is real and auditable.
 */
import { querySnowflake } from "@/lib/snowflake"
import { PX_FQN } from "@/lib/constants"

export const dynamic = "force-dynamic"

/** Coerce to a bare integer literal or NULL (no injection surface for numeric ids). */
function intLit(v: unknown): string {
  if (v === null || v === undefined || v === "") return "NULL"
  const n = Number(v)
  return Number.isFinite(n) ? String(Math.trunc(n)) : "NULL"
}

/** Escape a string for a single-quoted SQL literal. */
function strLit(v: unknown): string {
  if (v === null || v === undefined) return "NULL"
  return `'${String(v).slice(0, 400).replace(/'/g, "''")}'`
}

export async function POST(req: Request) {
  let body: any
  try {
    body = await req.json()
  } catch {
    return Response.json({ error: "Invalid request body" }, { status: 400 })
  }

  const actionType = String(body.actionType ?? "").trim()
  if (!actionType) return Response.json({ error: "Missing actionType" }, { status: 400 })

  const sql = `
    INSERT INTO ${PX_FQN}.ACTION_EVENT_LOG
      (ENCOUNTER_ID, PATIENT_ID, UNIT_ID, ACTION_TYPE, CHANNEL, STATUS, ASSIGNED_TEAM, NOTES)
    SELECT ${intLit(body.encounterId)}, ${intLit(body.patientId)}, ${intLit(body.unitId)},
           ${strLit(actionType)}, ${strLit(body.channel)}, 'OPEN', ${strLit(body.assignedTeam)}, ${strLit(body.notes)}
  `

  try {
    await querySnowflake(sql)
    // Return the just-created row so the client can show the ACTION_ID + lifecycle.
    const rows = await querySnowflake(
      `SELECT ACTION_ID, STATUS, ACTION_TYPE, CHANNEL, ASSIGNED_TEAM, ENCOUNTER_ID, UNIT_ID, CREATED_AT
         FROM ${PX_FQN}.ACTION_EVENT_LOG
        WHERE ACTION_TYPE = ${strLit(actionType)}
        ORDER BY CREATED_AT DESC LIMIT 1`,
    )
    return Response.json({ ok: true, record: rows[0] ?? null })
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Failed to log action"
    console.error(new Date().toISOString(), "[action] insert failed:", msg)
    return Response.json({ error: msg }, { status: 500 })
  }
}
