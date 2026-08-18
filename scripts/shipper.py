#!/usr/bin/env python3
"""Continuous tail daemon: watches files, ships new lines to ingest-api.

Replaces the manual/cron-driven ship_logs.py for ongoing ingestion.
Tracks a per-file (inode, byte offset) in a state file so it resumes
correctly across restarts, and detects both rotation styles:
  - rename+recreate (inode changes)
  - copytruncate (inode stays, size shrinks below the stored offset)
A trailing line with no newline yet (still being written) is held back
until it's complete, so partial lines are never shipped.

Usage:
    python3 scripts/shipper.py --path /var/log/myapp/*.log --interval 2
    python3 scripts/shipper.py --path /var/log/syslog --source syslog --once
"""
import argparse
import glob
import json
import os
import time

from ship_logs import send_batch

DEFAULT_STATE_FILE = os.path.expanduser("~/.log_utils/shipper_state.json")


def load_state(state_path: str) -> dict:
    if os.path.exists(state_path):
        with open(state_path) as f:
            return json.load(f)
    return {}


def save_state(state_path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    tmp_path = state_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f)
    os.replace(tmp_path, state_path)


def read_new_lines(path: str, state: dict) -> list[str]:
    st = os.stat(path)
    inode = st.st_ino
    size = st.st_size
    file_state = state.get(path, {"inode": None, "offset": 0})

    rotated = file_state["inode"] is not None and file_state["inode"] != inode
    truncated = size < file_state["offset"]
    if rotated or truncated:
        file_state = {"inode": inode, "offset": 0}
    else:
        file_state["inode"] = inode

    with open(path, "rb") as f:
        f.seek(file_state["offset"])
        chunk = f.read()

    if not chunk:
        state[path] = file_state
        return []

    if chunk.endswith(b"\n"):
        consumed = len(chunk)
        complete = chunk
    else:
        last_newline = chunk.rfind(b"\n")
        if last_newline == -1:
            # no complete line yet - leave offset untouched, wait for more
            state[path] = file_state
            return []
        consumed = last_newline + 1
        complete = chunk[:consumed]

    file_state["offset"] += consumed
    state[path] = file_state
    return [line.decode("utf-8", errors="replace") for line in complete.split(b"\n") if line]


def poll_once(paths: list[str], source_override: str | None, url: str, batch_size: int, state: dict) -> None:
    matched = sorted({p for pattern in paths for p in glob.glob(pattern)})
    for path in matched:
        try:
            lines = read_new_lines(path, state)
        except FileNotFoundError:
            continue
        if not lines:
            continue
        source = source_override or os.path.splitext(os.path.basename(path))[0]
        for i in range(0, len(lines), batch_size):
            batch = lines[i : i + batch_size]
            result = send_batch(url, source, batch)
            print(f"shipped {len(batch)} lines from {path} -> job {result['job_id']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", action="append", required=True, help="file path or glob, may repeat")
    parser.add_argument("--source", help="source name for all matched files; default: each file's basename")
    parser.add_argument("--url", default="http://localhost:8000/ingest")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between polls")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--once", action="store_true", help="poll once and exit, instead of looping")
    args = parser.parse_args()

    state = load_state(args.state_file)
    try:
        while True:
            poll_once(args.path, args.source, args.url, args.batch_size, state)
            save_state(args.state_file, state)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        save_state(args.state_file, state)


if __name__ == "__main__":
    main()
