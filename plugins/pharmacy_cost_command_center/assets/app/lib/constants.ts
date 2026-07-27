/** App title — displayed in the nav header and browser tab */
export const APP_TITLE = "Pharmacy Cost Command Center"

/** Path to the logo in /public (used in the header and as favicon) */
export const LOGO_SRC = "/icon.svg"

/**
 * Data product location — baked in at deploy time.
 *
 * App Runtime does NOT let you inject custom runtime env vars (app.yml has no
 * env: block), and the auto-injected SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA are
 * the app's own service context (SNOWFLAKE_SCHEMA = SERVICE_CONTEXT_NESTED_SCHEMA,
 * SNOWFLAKE_DATABASE empty) — NOT the data location. So the data product FQN is
 * baked into these two defaults by the app-deploy skill from config.json
 * (target_database / target_schema) before `snow app deploy`.
 *
 * PHARMACY_DB / PHARMACY_SCHEMA env overrides still work for local dev.
 * Object names + columns are identical across sandbox and every customer prod.
 */
// DATA-TARGET (rewritten at deploy time from config.json target_database/target_schema)
const DEFAULT_DATA_DB = "AA_HC_PAYER_DERIVED_DATA_PRODUCTS"
const DEFAULT_DATA_SCHEMA = "PHARMACY_COST_INTELLIGENCE_SANDBOX"

/** Treat empty-string env vars as unset (App Runtime injects some as ""). */
function envOr(name: string, fallback: string): string {
  const v = process.env[name]
  return v && v.trim() !== "" ? v : fallback
}

export const DATA_DB = envOr("PHARMACY_DB", DEFAULT_DATA_DB)
export const DATA_SCHEMA = envOr("PHARMACY_SCHEMA", DEFAULT_DATA_SCHEMA)

/** Cortex Agent that powers the "Ask the agent" drawer. */
export const AGENT_NAME = process.env.PHARMACY_AGENT ?? "AGT_PHARMACY_COST_COMMAND_CENTER"

/** Fully-qualified object name inside the data product schema. */
export function fq(object: string): string {
  return `${DATA_DB}.${DATA_SCHEMA}.${object}`
}
