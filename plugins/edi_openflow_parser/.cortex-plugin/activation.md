# EDI Openflow Parser — Activation

Welcome to the **EDI Openflow Parser** plugin. This plugin helps you build and extend EDI parsing pipelines on Snowflake using Openflow (or a Python UDF lite path).

## What This Plugin Does

1. **Extend**: Add support for new X12 transaction types with AI-assisted field map generation
2. **Deploy**: Build the NiFi NAR processor, wire the Openflow flow, or deploy the Python UDF alternative
3. **Monitor**: Check pipeline health, DT refresh status, and error counts

## Getting Started

Run one of the slash commands:
- `/edi:extend` — Add or customize a transaction format
- `/edi:deploy` — Build and deploy the pipeline
- `/edi:status` — Check pipeline health

## Prerequisites

- Snowflake account with Cortex AI access (for Gold layer AI enrichment)
- For Openflow path: Medium+ Openflow runtime
- For Python UDF lite path: No additional infrastructure needed
- Recommended: Fork of [x12-openflow-quickstart](https://github.com/sfc-gh-akelkar/x12-openflow-quickstart)

## Pre-Built Transaction Types

The plugin ships with field maps for these X12 HIPAA transaction types:
- **837** — Health Care Claim (Professional/Institutional)
- **835** — Claim Payment/Remittance Advice
- **834** — Benefit Enrollment and Maintenance
- **270/271** — Eligibility Inquiry/Response
- **276/277** — Claim Status Request/Response
