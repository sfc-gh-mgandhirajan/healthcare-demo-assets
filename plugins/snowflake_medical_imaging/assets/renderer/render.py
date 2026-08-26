#!/usr/bin/env python3
"""Deterministic renderer for the Snowflake for Medical Imaging platform.

Reads build_manifest.yaml + a per-customer config.json and writes one file per
object. No database access -- this is a pure text transform. The skills execute
the rendered SQL with `snow sql -f`.

Ported from the dicom_imaging_intelligence renderer, which in turn dropped the
Pharmacy Cost Command Center conformance-resolution layer. The platform OWNS its
source (a DICOM stage it scans itself), so the only thing a template needs to
resolve is where to put objects and what to name them.

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

    # FAIL LOUDLY ON AN UNKNOWN --only NAME.
    #
    # object_name is upper case in the manifest, and `--only checks` matched
    # nothing, printed `"rendered": []`, and exited 0. The stale file from an
    # earlier render was still on disk, so the deploy that followed applied old
    # SQL. I believed a fix was live when it had never been rendered -- twice.
    #
    # A filter that silently matches nothing is indistinguishable from success.
    known = {o["object_name"] for o in manifest["objects"]}
    if a.only:
        unknown = [n for n in a.only if n not in known]
        if unknown:
            close = [k for k in known
                     if any(n.upper() == k or n.upper() in k for n in unknown)]
            raise SystemExit(
                f"--only: no such object_name: {', '.join(unknown)}\n"
                + (f"did you mean: {', '.join(sorted(close))}?\n" if close else "")
                + f"known objects: {', '.join(sorted(known))}")
    if a.phase:
        phases = {o.get("phase") for o in manifest["objects"]}
        bad = [p for p in a.phase if p not in phases]
        if bad:
            raise SystemExit(
                f"--phase: no such phase: {', '.join(bad)}\n"
                f"known phases: {', '.join(sorted(p for p in phases if p))}")

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
        # Most objects render to <object_name>.sql. The dicomweb phase renders
        # spec.yaml and nginx.conf, which must keep their real names.
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
