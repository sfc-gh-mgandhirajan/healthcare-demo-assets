#!/bin/bash
set -euo pipefail

INPUT=$(cat)
TOOL_NAME="${TOOL_NAME:-}"

CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')
SQL=$(echo "$INPUT" | jq -r '.tool_input.sql // empty')
CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // empty' 2>/dev/null || echo "")
TEXT=$(echo "$INPUT" | jq -r '.tool_input.text // empty' 2>/dev/null || echo "")

CHECK_TEXT="$CMD $SQL $CONTENT $TEXT"

PHI_PATTERNS=(
    '[0-9]{3}-[0-9]{2}-[0-9]{4}'
    '\b[0-9]{9}\b'
    '\b(MRN|mrn)[:\s]*[A-Z0-9]{5,}\b'
    'patient[_\s]?(name|id|birth|dob|ssn|mrn)'
    '\b[A-Z][a-z]+,\s[A-Z][a-z]+\s(DOB|MRN|SSN)'
)

for pattern in "${PHI_PATTERNS[@]}"; do
    if echo "$CHECK_TEXT" | grep -qEi "$pattern"; then
        echo "BLOCKED: Potential PHI detected in tool input. Pattern matched: $pattern. Review the content before proceeding — PHI must not be logged, committed, or sent to external services." >&2
        exit 2
    fi
done

if echo "$CMD" | grep -qEi '(SELECT|INSERT|UPDATE|DELETE|COPY).*patient_(name|id|birth)'; then
    echo "BLOCKED: SQL query references PHI columns (patient_name, patient_id, patient_birth_date). Use masked views or apply WHERE clause filtering." >&2
    exit 2
fi

if echo "$SQL" | grep -qEi '(SELECT|INSERT|UPDATE|DELETE|COPY).*patient_(name|id|birth)'; then
    echo "BLOCKED: SQL query references PHI columns (patient_name, patient_id, patient_birth_date). Use masked views or apply WHERE clause filtering." >&2
    exit 2
fi

exit 0
