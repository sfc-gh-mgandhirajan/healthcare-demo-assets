#!/usr/bin/env python3
"""Deterministic renderer for the DICOM Imaging Intelligence plugin.

Reads build_manifest.yaml + a per-customer config.json and writes one .sql file
per object. No database access -- this is a pure text transform. The skills
execute the rendered SQL with `snow sql -f`.

Adapted from the Pharmacy Cost Command Center renderer with the entire
conformance-resolution layer removed. That plugin maps onto UNKNOWN customer
schemas, so it needs tbl()/c()/col()/join()/canon()/expr() and a bindings dump
from three conformance tables. This solution OWNS its source (a DICOM stage it
scans itself), so the only thing a template needs to resolve is where to put
objects and what to name them.

Template helpers exposed as Jinja globals:
  cfg                  -> the whole config dict (use cfg.<key>)
  target(object_name)  -> TARGET_DATABASE.TARGET_SCHEMA.OBJECT_NAME
  db / schema          -> cfg.target_database / cfg.target_schema
  warehouse            -> cfg.warehouse
  owner_role           -> cfg.owner_role

StrictUndefined is deliberate: a template that references a config key which
config.json does not define fails HERE, at render time, instead of emitting
"None" into a CREATE statement and failing halfway through a deploy.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

# Keys every render needs. Checked up front so a missing one produces a clear
# message instead of a Jinja traceback from inside a template.
REQUIRED_KEYS = [
    "target_database",
    "target_schema",
    "warehouse",
    "owner_role",
]


def load_config(path):
    with open(path) as fh:
        try:
            cfg = json.load(fh)
        except json.JSONDecodeError as exc:
            sys.exit(
                f"FAILED: {path} is not valid JSON ({exc}).\n"
                "config.json is read as strict JSON - it may not contain comments."
            )

    missing = [k for k in REQUIRED_KEYS if not cfg.get(k)]
    if missing:
        sys.exit(
            f"FAILED: {path} is missing required key(s): {', '.join(missing)}.\n"
            "Copy assets/config.sample.json and fill it in."
        )
    return cfg


def build_env(templates_dir, config):
    def target(obj):
        return f"{config['target_database']}.{config['target_schema']}.{obj}"

    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        undefined=StrictUndefined,
        trim_blocks=False,
        lstrip_blocks=False,
        keep_trailing_newline=True,
    )
    env.globals.update(
        cfg=config,
        target=target,
        db=config["target_database"],
        schema=config["target_schema"],
        warehouse=config["warehouse"],
        owner_role=config["owner_role"],
    )
    return env


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--templates", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--only", nargs="*", help="render only these object_names")
    ap.add_argument("--phase", nargs="*", help="render only these phases")
    a = ap.parse_args()

    config = load_config(a.config)
    env = build_env(a.templates, config)
    manifest = yaml.safe_load(open(a.manifest))

    os.makedirs(a.out, exist_ok=True)
    rendered = []
    skipped = []
    for obj in manifest["objects"]:
        name = obj["object_name"]
        if a.only and name not in a.only:
            continue
        if a.phase and obj.get("phase") not in a.phase:
            continue
        # An object can opt out via a config flag, e.g. the bundled IDC demo
        # source when a customer only wants their own stage.
        gate = obj.get("enabled_if")
        if gate and not config.get(gate):
            skipped.append({"object": name, "reason": f"{gate} is false"})
            continue

        tmpl = env.get_template(obj["template"])
        sql = tmpl.render(obj=obj, params=obj.get("params", {}))
        # Most objects render to <object_name>.sql. The app phase renders
        # snowflake.yml and app_config.json, which must keep their real names.
        out_name = obj.get("out_file", f"{name}.sql")
        outp = Path(a.out) / out_name
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(sql)
        rendered.append(
            {
                "object": name,
                "phase": obj.get("phase"),
                "role": obj.get("role"),
                "path": str(outp),
            }
        )

    print(json.dumps({"rendered": rendered, "skipped": skipped}, indent=2))


if __name__ == "__main__":
    main()
