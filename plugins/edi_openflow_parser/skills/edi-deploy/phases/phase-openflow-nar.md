---
name: phase-openflow-nar
description: Build the NiFi NAR processor package
parent_skill: edi-deploy
phase_id: DEPLOY_P1
---

# Phase: NAR Build

## Purpose
Build the NiFi NAR (NiFi Archive) file containing the ParseX12ToJSON processor and all field maps.

## Steps

1. **Verify build tools**:
   ```bash
   pip install hatch hatch-datavolo-nar
   ```

2. **Verify pyproject.toml**:
   - `build-system.requires` includes `hatch-datavolo-nar`
   - `dependencies` lists `pyx12` (no version specifier — NAR builder chokes on `>=`)
   - `tool.hatch.build.targets.nar.packages` points to `src/x12_processors`

3. **Build NAR**:
   ```bash
   cd {plugin_root}
   hatch build --target nar
   ```

4. **Verify output**:
   - NAR file at `dist/x12_processors-{version}.nar`
   - Size should be 800KB+ (includes bundled pyx12)
   - If version bump needed: update `src/x12_processors/__about__.py`

5. **Upload to Openflow**:
   - Navigate to Openflow UI → Extensions → Upload NAR
   - Or use Openflow API if available
   - Wait for processor to appear in palette

## Key Gotchas

- **hatch-datavolo-nar wraps dependency specs in single quotes** — version specifiers like `>=4.0.0` cause pip install failure inside the NAR build. Use plain package names only.
- **Bump version** if re-uploading — Openflow caches by version number. Change `__about__.py` version to force reload.
- **Python import path**: The processor uses `try/except` import pattern because NiFi flattens Python files. Our code handles both `from x12_processors.field_maps import ...` and `from field_maps import ...`.

## Output
- NAR file path for upload
- Version number for tracking
