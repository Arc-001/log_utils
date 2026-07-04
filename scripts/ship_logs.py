#!/usr/bin/env python3
"""Manual test shipper: read a log file and POST batches to ingest-api.

Usage:
    python3 scripts/ship_logs.py --file /var/log/nginx/access.log --source nginx-access
"""
import argparse
import json
import urllib.request


def send_batch(url: str, source: str, lines: list[str]) -> dict:
    body = json.dumps({"source": source, "lines": lines}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="path to log file to ship")
    parser.add_argument("--source", required=True, help="source name, e.g. nginx-access")
    parser.add_argument("--url", default="http://localhost:8000/ingest")
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()

    with open(args.file, "r", errors="replace") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]

    if not lines:
        print("no lines found, nothing to ship")
        return

    for i in range(0, len(lines), args.batch_size):
        batch = lines[i : i + args.batch_size]
        result = send_batch(args.url, args.source, batch)
        print(f"shipped {len(batch)} lines -> job {result['job_id']}")


if __name__ == "__main__":
    main()
