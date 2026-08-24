#!/bin/bash
# enforce-contract.sh — PreToolUse hook for edi-openflow-parser
# Blocks operations that violate the execution contract.
#
# CoCo passes hook context as JSON on stdin. Exit code 2 blocks the action
# (with reason on stderr). Any other exit code allows the action to proceed.

INPUT=$(cat)
SQL=$(echo "$INPUT" | jq -r '.tool_input.sql // .tool_input.command // empty' 2>/dev/null)

if [ -z "$SQL" ]; then
  exit 0
fi

# Block PutSnowpipeStreaming2 references
if echo "$SQL" | grep -qi "PutSnowpipeStreaming2"; then
  echo "Use PutSnowpipeStreaming v1 (Record Reader + Table target), not PutSnowpipeStreaming2." >&2
  exit 2
fi

# Block manual NAR packaging
if echo "$SQL" | grep -qi "zip.*\.nar\|jar.*\.nar"; then
  echo "Use 'hatch build --target nar' for NAR packaging. Never manual zip/jar." >&2
  exit 2
fi

# Block network policy changes outside the deploy skill
if echo "$SQL" | grep -qi "ALTER.*NETWORK.*POLICY\|CREATE.*NETWORK.*POLICY"; then
  echo "Network policy changes must go through the edi-deploy network verification phase. Run /edi:deploy first." >&2
  exit 2
fi

exit 0
