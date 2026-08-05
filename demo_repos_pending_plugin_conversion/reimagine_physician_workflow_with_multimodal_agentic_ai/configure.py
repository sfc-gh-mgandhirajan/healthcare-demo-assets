#!/usr/bin/env python3
"""Customize all project files for your Snowflake environment.

Usage:
    python3 configure.py                                    # Interactive prompts
    python3 configure.py --db MY_DB --schema MY_SCHEMA      # Direct
    python3 configure.py --db MY_DB --schema MY_SCHEMA --account myorg-myaccount \
        --warehouse MY_WH --interactive-wh MY_INTERACTIVE_WH \
        --image-stage MY_DB.MY_SCHEMA.ECG_STAGE \
        --agent-db SNOWFLAKE_INTELLIGENCE --agent-schema AGENTS --agent-name MY_AGENT \
        --agent-model claude-4-sonnet
"""
import argparse
import os
import re

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

DEFAULTS = {
    "db": "DEMO_DB",
    "schema": "HIMSS_DEMO",
    "account": "myorg-myaccount",
    "warehouse": "DEMO_BUILD_WH",
    "interactive_wh": "HIMSS_INTERACTIVE_WH",
    "image_stage": "MEDGEMMA_DEMO.PUBLIC.ECG_STAGE",
    "agent_db": "SNOWFLAKE_INTELLIGENCE",
    "agent_schema": "AGENTS",
    "agent_name": "HIMSS_PHYSICIAN_AGENT",
    "agent_model": "claude-4-sonnet",
}


def regex_replace_in_file(path, patterns):
    with open(path, "r") as f:
        content = f.read()
    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)
    with open(path, "w") as f:
        f.write(content)


def configure(db, schema, account, warehouse, interactive_wh, image_stage,
              agent_db, agent_schema, agent_name, agent_model):
    changes = []

    yaml_path = os.path.join(REPO_ROOT, "sql", "himss_patient_semantic_model.yaml")
    if os.path.exists(yaml_path):
        with open(yaml_path, "r") as f:
            content = f.read()
        old_db = re.search(r"database: (\w+)", content)
        old_schema = re.search(r"schema: (\w+)", content)
        if old_db and old_schema:
            prev_db, prev_schema = old_db.group(1), old_schema.group(1)
            old_fqn = f"{prev_db}.{prev_schema}"
            new_fqn = f"{db}.{schema}"
            content = content.replace(f"database: {prev_db}", f"database: {db}")
            content = content.replace(f"schema: {prev_schema}", f"schema: {schema}")
            content = content.replace(f"{old_fqn}.", f"{new_fqn}.")
            with open(yaml_path, "w") as f:
                f.write(content)
            changes.append(f"  sql/himss_patient_semantic_model.yaml  ({old_fqn} -> {new_fqn})")

    setup_path = os.path.join(REPO_ROOT, "sql", "setup_data.sql")
    if os.path.exists(setup_path):
        regex_replace_in_file(setup_path, [
            (r"SET MY_DB = '[^']*';", f"SET MY_DB = '{db}';"),
            (r"SET MY_SCHEMA = '[^']*';", f"SET MY_SCHEMA = '{schema}';"),
            (r"SET MY_WAREHOUSE = '[^']*';", f"SET MY_WAREHOUSE = '{warehouse}';"),
            (r"SET IMAGE_STAGE = '[^']*';", f"SET IMAGE_STAGE = '{image_stage}';"),
        ])
        changes.append(f"  sql/setup_data.sql  (MY_DB={db}, MY_SCHEMA={schema}, MY_WAREHOUSE={warehouse}, IMAGE_STAGE={image_stage})")

    deploy_path = os.path.join(REPO_ROOT, "sql", "deploy_medgemma.sql")
    if os.path.exists(deploy_path):
        regex_replace_in_file(deploy_path, [
            (r"SET MY_DB = '[^']*';", f"SET MY_DB = '{db}';"),
            (r"SET MY_SCHEMA = '[^']*';", f"SET MY_SCHEMA = '{schema}';"),
        ])
        changes.append(f"  sql/deploy_medgemma.sql  (MY_DB={db}, MY_SCHEMA={schema})")

    env_path = os.path.join(REPO_ROOT, "himss-physician-app", ".env.example")
    if os.path.exists(env_path):
        regex_replace_in_file(env_path, [
            (r"VITE_SNOWFLAKE_ACCOUNT=.*", f"VITE_SNOWFLAKE_ACCOUNT={account}"),
            (r"VITE_SNOWFLAKE_DATABASE=.*", f"VITE_SNOWFLAKE_DATABASE={db}"),
            (r"VITE_SNOWFLAKE_SCHEMA=.*", f"VITE_SNOWFLAKE_SCHEMA={schema}"),
            (r"VITE_SNOWFLAKE_WAREHOUSE=.*", f"VITE_SNOWFLAKE_WAREHOUSE={interactive_wh}"),
            (r"VITE_AGENT_DATABASE=.*", f"VITE_AGENT_DATABASE={agent_db}"),
            (r"VITE_AGENT_SCHEMA=.*", f"VITE_AGENT_SCHEMA={agent_schema}"),
            (r"VITE_AGENT_NAME=.*", f"VITE_AGENT_NAME={agent_name}"),
            (r"VITE_AGENT_MODEL=.*", f"VITE_AGENT_MODEL={agent_model}"),
        ])
        changes.append(f"  himss-physician-app/.env.example  (account={account}, db={db}, schema={schema}, wh={interactive_wh}, agent={agent_db}.{agent_schema}.{agent_name})")

    return changes


def main():
    parser = argparse.ArgumentParser(description="Configure PhysicianAssist for your Snowflake environment")
    parser.add_argument("--db", help=f"Database name (default: {DEFAULTS['db']})")
    parser.add_argument("--schema", help=f"Schema name (default: {DEFAULTS['schema']})")
    parser.add_argument("--account", help=f"Snowflake account identifier (default: {DEFAULTS['account']})")
    parser.add_argument("--warehouse", help=f"Build warehouse (default: {DEFAULTS['warehouse']})")
    parser.add_argument("--interactive-wh", help=f"Interactive query warehouse (default: {DEFAULTS['interactive_wh']})")
    parser.add_argument("--image-stage", help=f"Fully-qualified image stage (default: {DEFAULTS['image_stage']})")
    parser.add_argument("--agent-db", help=f"Agent database (default: {DEFAULTS['agent_db']})")
    parser.add_argument("--agent-schema", help=f"Agent schema (default: {DEFAULTS['agent_schema']})")
    parser.add_argument("--agent-name", help=f"Agent name (default: {DEFAULTS['agent_name']})")
    parser.add_argument("--agent-model", help=f"Agent model (default: {DEFAULTS['agent_model']})")
    args = parser.parse_args()

    db = args.db or input(f"Database name [{DEFAULTS['db']}]: ").strip() or DEFAULTS["db"]
    schema = args.schema or input(f"Schema name [{DEFAULTS['schema']}]: ").strip() or DEFAULTS["schema"]
    account = args.account or input(f"Snowflake account [{DEFAULTS['account']}]: ").strip() or DEFAULTS["account"]
    warehouse = args.warehouse or input(f"Build warehouse [{DEFAULTS['warehouse']}]: ").strip() or DEFAULTS["warehouse"]
    interactive_wh = args.interactive_wh or input(f"Interactive warehouse [{DEFAULTS['interactive_wh']}]: ").strip() or DEFAULTS["interactive_wh"]
    image_stage = args.image_stage or input(f"Image stage [{DEFAULTS['image_stage']}]: ").strip() or DEFAULTS["image_stage"]
    agent_db = args.agent_db or input(f"Agent database [{DEFAULTS['agent_db']}]: ").strip() or DEFAULTS["agent_db"]
    agent_schema = args.agent_schema or input(f"Agent schema [{DEFAULTS['agent_schema']}]: ").strip() or DEFAULTS["agent_schema"]
    agent_name = args.agent_name or input(f"Agent name [{DEFAULTS['agent_name']}]: ").strip() or DEFAULTS["agent_name"]
    agent_model = args.agent_model or input(f"Agent model [{DEFAULTS['agent_model']}]: ").strip() or DEFAULTS["agent_model"]

    print(f"\nConfiguring for: {db}.{schema} on {account}")
    print(f"  Build warehouse: {warehouse}, Interactive warehouse: {interactive_wh}")
    print(f"  Image stage: {image_stage}")
    print(f"  Agent: {agent_db}.{agent_schema}.{agent_name} (model: {agent_model})\n")
    changes = configure(db, schema, account, warehouse, interactive_wh, image_stage,
                        agent_db, agent_schema, agent_name, agent_model)

    if changes:
        print("Updated files:")
        for c in changes:
            print(c)
        print(f"\nNext steps:")
        print(f"  1. cd himss-physician-app && cp .env.example .env.local")
        print(f"  2. Edit .env.local — add your Snowflake PAT")
        print(f"  3. Run sql/deploy_medgemma.sql in Snowflake")
        print(f"  4. Run sql/setup_data.sql in Snowflake")
    else:
        print("No files needed updating.")


if __name__ == "__main__":
    main()
