/** App title — displayed in the nav header and browser tab */
export const APP_TITLE = "Patient Experience Command Center"

/** Path to the logo in /public (used in the header and as favicon) */
export const LOGO_SRC = "/icon.svg"

/**
 * Target database / schema for the certified Derived Patient Experience Data Product.
 * These are PLACEHOLDER defaults — the app-deploy skill rewrites PX_DATABASE / PX_SCHEMA
 * from config.json at deploy time (App Runtime cannot inject runtime env vars).
 */
export const PX_DATABASE = "CUSTOMER_DERIVED_DB"
export const PX_SCHEMA = "PATIENT_EXPERIENCE"
export const PX_AGENT = "AGT_PATIENT_EXPERIENCE"
export const PX_FQN = `${PX_DATABASE}.${PX_SCHEMA}`
