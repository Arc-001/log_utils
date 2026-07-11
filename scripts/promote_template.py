#!/usr/bin/env python3
"""Edit a template's regex and/or promote/demote its status.

Usage:
    python3 scripts/promote_template.py --id 30 --regex '(?P<foo>...)' --status active
    python3 scripts/promote_template.py --id 17 --status review

After promoting, restart trusted-worker before replaying affected raw
objects: it caches resolved template state in memory per cluster for
the life of the process, so it won't pick up the DB change until it
restarts (same as its Drain3 clustering state resetting on restart).
    docker compose restart trusted-worker

Then use scripts/replay.py to re-run the affected raw object(s).
"""
import argparse
import subprocess

from _env import load_env


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, help="templates.id to edit")
    parser.add_argument("--regex", help="new regex (Python re syntax)")
    parser.add_argument("--status", choices=["active", "review"])
    args = parser.parse_args()

    if not args.regex and not args.status:
        parser.error("specify --regex and/or --status")

    status = args.status or ("active" if args.regex else None)

    set_clauses = []
    if args.regex:
        set_clauses.append(f"regex = {sql_string_literal(args.regex)}")
    if status:
        set_clauses.append(f"status = {sql_string_literal(status)}")

    sql = (
        f"UPDATE templates SET {', '.join(set_clauses)} "
        f"WHERE id = {args.id} RETURNING id, signature, status, regex;"
    )

    env = load_env()
    user = env.get("POSTGRES_USER", "log_utils")
    db = env.get("POSTGRES_DB", "log_utils")

    subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", user, "-d", db, "-c", sql],
        check=True,
    )


if __name__ == "__main__":
    main()
