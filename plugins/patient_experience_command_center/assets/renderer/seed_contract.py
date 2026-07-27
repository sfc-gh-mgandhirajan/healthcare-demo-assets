#!/usr/bin/env python3
"""Seed the conformance ELEMENTS/RELATIONSHIPS skeleton from the canonical contract.

Seeds ONLY the contract-invariant descriptive columns (ELEMENT_NAME, ELEMENT_TIER,
LOGICAL_OBJECT, DATA_TYPE=expected canonical type, IS_REQUIRED / relationship
metadata) straight from assets/contract/*.json. This prevents those columns (e.g.
ELEMENT_NAME) from ever being left NULL.

Deliberately does NOT seed any customer/discovery-owned column: the physical
mapping (SOURCE_DATABASE/SCHEMA/OBJECT/COLUMN, STAGE_PATH, FULLY_QUALIFIED_REF),
the discovered SOURCE_GRAIN, TRANSFORM_EXPRESSION/NOTES, or MAPPING_STATUS
(stays PROPOSED until review). Phase-1 discovery fills those by profiling the
customer's source — the seeder must not pre-empt discovery.

Emits idempotent MERGE SQL to stdout (or --out file). The MERGE:
  - INSERTs one skeleton row per contract element/relationship (MAPPING_STATUS='PROPOSED'),
  - on re-run, UPDATEs ONLY the descriptive columns (never touches SOURCE_*/MAPPING_STATUS),
so it is safe to re-run after discovery has begun.

Usage:
  python3 seed_contract.py --contract <contract_dir> --database <DB> --schema <SCHEMA> [--out seed.sql]

No database access — pure text transform. The conformance-discover skill runs the
emitted SQL via `snow sql -f`.
"""
import argparse, json, os


def q(v):
    """SQL literal for a string/None."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def b(v):
    return "TRUE" if v else "FALSE"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract", required=True, help="dir with logical_elements.json / logical_objects.json / logical_relationships.json")
    ap.add_argument("--database", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--out")
    a = ap.parse_args()

    elements = json.load(open(os.path.join(a.contract, "logical_elements.json")))
    objects = {o["id"]: o for o in json.load(open(os.path.join(a.contract, "logical_objects.json")))["objects"]}
    rels = json.load(open(os.path.join(a.contract, "logical_relationships.json")))["relationships"]

    et = f"{a.database}.{a.schema}.ACCELERATOR_CORE_ELEMENTS"
    rt = f"{a.database}.{a.schema}.ACCELERATOR_CORE_RELATIONSHIPS"

    # ---- ELEMENTS skeleton (descriptive columns from the contract) ----
    # Seed ONLY contract-invariant columns. SOURCE_GRAIN and the physical SOURCE_*
    # mapping are discovery-owned and are intentionally left NULL here.
    erows = []
    for e in elements:
        erows.append("(" + ", ".join([
            q(e["LOGICAL_ID"]), q(e.get("LABEL")), q(e.get("TIER")),
            q(e.get("LOGICAL_OBJECT")), q(e.get("DATA_TYPE")), b(e.get("IS_REQUIRED")),
        ]) + ")")
    evals = ",\n    ".join(erows)
    elem_sql = f"""MERGE INTO {et} t
USING (
  SELECT * FROM VALUES
    {evals}
  AS v(ELEMENT_ID, ELEMENT_NAME, ELEMENT_TIER, LOGICAL_OBJECT, DATA_TYPE, IS_REQUIRED)
) v
ON t.ELEMENT_ID = v.ELEMENT_ID
WHEN MATCHED THEN UPDATE SET
  t.ELEMENT_NAME = v.ELEMENT_NAME, t.ELEMENT_TIER = v.ELEMENT_TIER,
  t.LOGICAL_OBJECT = v.LOGICAL_OBJECT, t.DATA_TYPE = COALESCE(t.DATA_TYPE, v.DATA_TYPE),
  t.IS_REQUIRED = v.IS_REQUIRED, t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (ELEMENT_ID, ELEMENT_NAME, ELEMENT_TIER, LOGICAL_OBJECT, DATA_TYPE, IS_REQUIRED, MAPPING_STATUS)
  VALUES (v.ELEMENT_ID, v.ELEMENT_NAME, v.ELEMENT_TIER, v.LOGICAL_OBJECT, v.DATA_TYPE, v.IS_REQUIRED, 'PROPOSED');"""

    # ---- RELATIONSHIPS skeleton (logical endpoints + metadata from the contract) ----
    rrows = []
    for r in rels:
        rrows.append("(" + ", ".join([
            q(r["RELATIONSHIP_ID"]), q(r.get("LEFT_OBJECT")), q(r.get("RIGHT_OBJECT")),
            q(r.get("CARDINALITY")), q(r.get("REL_KIND", "equi")), b(r.get("REQUIRED")), q(r.get("PURPOSE")),
        ]) + ")")
    rvals = ",\n    ".join(rrows)
    rel_sql = f"""MERGE INTO {rt} t
USING (
  SELECT * FROM VALUES
    {rvals}
  AS v(RELATIONSHIP_ID, LEFT_OBJECT, RIGHT_OBJECT, CARDINALITY, REL_KIND, IS_REQUIRED, PURPOSE)
) v
ON t.RELATIONSHIP_ID = v.RELATIONSHIP_ID
WHEN MATCHED THEN UPDATE SET
  t.CARDINALITY = v.CARDINALITY, t.REL_KIND = v.REL_KIND, t.IS_REQUIRED = v.IS_REQUIRED,
  t.PURPOSE = v.PURPOSE, t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (RELATIONSHIP_ID, LEFT_OBJECT, RIGHT_OBJECT, CARDINALITY, REL_KIND, IS_REQUIRED, PURPOSE, MAPPING_STATUS)
  VALUES (v.RELATIONSHIP_ID, v.LEFT_OBJECT, v.RIGHT_OBJECT, v.CARDINALITY, v.REL_KIND, v.IS_REQUIRED, v.PURPOSE, 'PROPOSED');"""

    out = elem_sql + "\n\n" + rel_sql + "\n"
    if a.out:
        open(a.out, "w").write(out)
        print(f"wrote {a.out} ({len(elements)} elements, {len(rels)} relationships)")
    else:
        print(out)


if __name__ == "__main__":
    main()
