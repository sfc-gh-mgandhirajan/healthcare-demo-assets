---
name: gate-runtime
description: Verify Openflow runtime exists and meets requirements
parent_skill: edi-deploy
gate_id: DEPLOY_G1
---

# Gate: Openflow Runtime Verification

## Purpose
Verify the Openflow runtime is available, sized correctly, and ready for NAR deployment.

## Steps

1. **Check Openflow deployment**: Ask user for their deployment name or detect from context
2. **Verify runtime size**: Must be MEDIUM or larger (Python processors need the extra memory)
3. **Check NAR upload capability**: Verify the user has permissions to upload extensions
4. **Verify network connectivity**: Confirm SPCS egress to source (S3/SFTP)

## Key Constraints
- Python processors require Medium+ runtime (not Small)
- NAR upload requires appropriate role permissions on the Openflow deployment
- Runtime must be in RUNNING state to accept new extensions

## Pass Criteria
- Openflow deployment identified
- Runtime is Medium+ and RUNNING
- User has extension upload permissions

## Failure Handling
- No Openflow runtime: suggest Python UDF lite path instead
- Runtime too small: provide ALTER statement to resize
- Runtime stopped: provide resume command
