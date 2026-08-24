---
name: gate-repo
description: Locate the plugin's bundled parsing engine and set output mode
parent_skill: edi-extend
gate_id: G2
---

# Gate: Plugin Source Detection

## Purpose
Locate the plugin's bundled parsing engine (`src/x12_processors/`) and determine the output mode for generated code.

## Steps

1. **Locate plugin root**: Find the `edi-openflow-parser` directory containing:
   - `pyproject.toml` with `hatch-datavolo-nar` in build-system requires
   - `src/x12_processors/ParseX12ToJSON.py`
   - `src/x12_processors/field_maps.py`
   - `config/x12_known_types.yaml`

2. **If found**: Report location, confirm this is the working copy. The plugin is self-contained — all source lives here.

3. **If not found** (plugin was loaded from catalog but user wants to modify source):
   ```
   The plugin's parsing engine is bundled but not editable in your current workspace.
   
   Options:
   A) Clone the plugin repo locally for editing (git clone https://github.com/sfc-gh-akelkar/edi-openflow-parser)
   B) Continue in dry-run mode (I'll output code blocks for you to apply)
   ```

4. **Set output mode**:
   - Plugin source found in workspace → write mode (default)
   - User preference or no writable source → dry-run mode

## Pass Criteria
- Plugin root located with parser source present, OR
- Output mode set to dry-run

## Context for Next Gates
Pass the following to subsequent phases:
- `plugin_root`: absolute path to plugin directory
- `field_maps_path`: path to `src/x12_processors/field_maps.py`
- `sql_dir`: path to `sql/` directory
- `tests_dir`: path to `tests/` directory
- `config_dir`: path to `config/` directory
- `output_mode`: "write" or "dry_run"
