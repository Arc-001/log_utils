#!/usr/bin/env python3
"""Re-run raw MinIO objects through trusted-worker (replay).

Use this after promoting a review-status template (see
promote_template.py) to backfill lines that were previously skipped, or
generally to re-process a source's raw objects with the latest set of
templates. Republishing is idempotent: refined-loader dedupes on
line_hash, so already-structured lines won't be duplicated.

Usage:
    python3 scripts/replay.py --source kernel
    python3 scripts/replay.py --object-key kernel/2026-07-04/20/abc123.ndjson.gz
"""
import argparse
import base64
import json
import subprocess
import urllib.request

from _env import load_env


def list_objects(source: str) -> list[str]:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "minio", "mc", "ls", "--recursive", f"local/raw-logs/{source}"],
        capture_output=True,
        text=True,
        check=True,
    )
    keys = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        relative_key = line.split()[-1]
        keys.append(f"{source}/{relative_key}")
    return keys


def publish_trusted_job(rabbitmq_user: str, rabbitmq_pass: str, source: str, object_key: str) -> None:
    auth = base64.b64encode(f"{rabbitmq_user}:{rabbitmq_pass}".encode()).decode()
    body = json.dumps(
        {
            "properties": {},
            "routing_key": "trusted.jobs",
            "payload": json.dumps({"job_id": "replay", "source": source, "object_key": object_key}),
            "payload_encoding": "string",
        }
    ).encode()
    req = urllib.request.Request(
        "http://localhost:15672/api/exchanges/%2F/log.events/publish",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Basic {auth}"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        resp.read()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="replay every raw object under this source")
    parser.add_argument("--object-key", help="replay a single specific raw object key")
    args = parser.parse_args()

    if not args.source and not args.object_key:
        parser.error("specify --source or --object-key")

    env = load_env()
    rabbitmq_user = env.get("RABBITMQ_DEFAULT_USER", "log_utils")
    rabbitmq_pass = env.get("RABBITMQ_DEFAULT_PASS", "")

    if args.object_key:
        source = args.object_key.split("/", 1)[0]
        object_keys = [args.object_key]
    else:
        source = args.source
        object_keys = list_objects(args.source)

    print(f"replaying {len(object_keys)} object(s) for source={source}")
    for object_key in object_keys:
        publish_trusted_job(rabbitmq_user, rabbitmq_pass, source, object_key)
        print(f"  queued {object_key}")


if __name__ == "__main__":
    main()
