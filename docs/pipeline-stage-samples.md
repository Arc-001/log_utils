# Pipeline stage samples

Real data pulled from a manual test run on this machine (2026-07-05, `PHASE4`
branch) after shipping `journalctl`/`dmesg`-derived logs plus a synthetic app
log through the pipeline. Shows what the same log line looks like at each of
the three stages.

## 1. Raw zone (MinIO)

Object: `raw-logs/systemd-boot/2026-07-04/20/2ecab4f6-8548-49b7-a22c-0b32208d7107.ndjson.gz`

Gzipped NDJSON, one line per raw log line, untouched except for line
numbering and the batch's ingestion metadata:

```json
{"line_no": 0, "line": "2026-07-05T04:41:38+05:30 fedora systemd[1]: Reached target initrd-usr-fs.target - Initrd /usr File System.", "source": "systemd-boot", "received_at": "2026-07-04T20:48:38.401583+00:00"}
{"line_no": 1, "line": "2026-07-05T04:41:38+05:30 fedora systemd[1]: Reached target slices.target - Slice Units.", "source": "systemd-boot", "received_at": "2026-07-04T20:48:38.401583+00:00"}
{"line_no": 2, "line": "2026-07-05T04:41:38+05:30 fedora systemd[1]: Reached target swap.target - Swaps.", "source": "systemd-boot", "received_at": "2026-07-04T20:48:38.401583+00:00"}
```

400 lines in this object. Nothing is parsed or interpreted yet - this is the
immutable, replayable source of truth.

## 2. Trusted zone (`templates` table in Postgres)

Drain3 clusters raw lines by shape; once a cluster accumulates enough
samples, OpenRouter generates a named-group regex + field schema, which gets
validated (compiled + match-rate checked) before being marked `active`.
29 templates were generated across all test sources; a representative
spread:

| id | status | regex | fields_schema | sample line |
|----|--------|-------|---------------|-------------|
| 4 | active | `(?P<timestamp>\S+) (?P<hostname>\S+) (?P<facility>kernel): (?P<interface>\S+): port (?P<port_num>\d+)\((?P<veth_name>\w+)\) entered (?P<state>\w+) state` | `{state, facility, hostname, port_num:int, interface, timestamp, veth_name}` | `2026-07-05T00:58:14+05:30 fedora kernel: br-0f16aaa2ee31: port 2(vethc9eacfc) entered forwarding state` |
| 5 | active | `(?P<timestamp>...) (?P<hostname>\S+) (?P<process>\w+): (?P<new_interface>\w+): renamed from (?P<old_interface>\w+)` | `{process, hostname, timestamp, new_interface, old_interface}` | `2026-07-05T00:58:19+05:30 fedora kernel: vethcd96550: renamed from eth0` |
| 8 | active | `(?P<timestamp>\S+) (?P<hostname>\S+) (?P<process>\S+)\[(?P<pid>\d+)\]: Starting (?P<service>\S+) - (?P<description>.*?)(?:\.\.\.\|$)` | `{pid:int, process, service, hostname, timestamp, description}` | `2026-07-05T04:41:38+05:30 fedora systemd[1]: Starting systemd-modules-load.service - Load Kernel Modules...` |
| 2 | active | `(?P<timestamp>\S+) (?P<app>\S+) (?P<level>\S+) user=(?P<user>\S+) action=(?P<action>\S+) status=(?P<status>\S+) latency_ms=(?P<latency_ms>\d+)` | `{app, user, level, action, status, timestamp:timestamp, latency_ms:int}` | `2026-07-05T10:00:01Z myapp INFO user=alice action=login status=success latency_ms=123` |
| 17 | **review** | `(?!)` (never matches - placeholder) | `{}` | `2026-07-05T04:41:45+05:30 fedora systemd[1]: Starting auditd.service - Security Audit Logging Service...` |
| 30 | **review** | `(?P<syslog_timestamp>\S+) ... (?P<message>.*?) state=(?P<state>.+)` | `{pid:int, level, state, module, message, ...}` | `2026-07-05T04:41:52+05:30 fedora warp-svc[1099]: ... Stopping WarpConnection state=Disconnected` |

Rows 17/18 landed in `review` because the OpenRouter call itself hit a
30s request timeout (caught, fell back to the `(?!)` placeholder regex, no
crash). Row 30 landed in `review` because the generated regex matched only
some of the 5 buffered samples (real `warp-svc` debug lines are structurally
noisier than they look) - the validation gate correctly benched it instead
of silently applying a bad template.

## 3. Refined zone (`refined.jobs` records, pre-Postgres-load)

Once a template is `active`, matching lines get regex-applied and published
as structured records. Same three example log lines as above, now
structured:

```json
{"source": "kernel", "template_id": 4, "ts": "2026-07-04T20:43:04.596797+00:00",
 "raw_message": "2026-07-05T00:58:32+05:30 fedora kernel: br-0f16aaa2ee31: port 2(veth6787c8d) entered blocking state",
 "fields": {"timestamp": "2026-07-05T00:58:32+05:30", "hostname": "fedora", "facility": "kernel",
            "interface": "br-0f16aaa2ee31", "port_num": "2", "veth_name": "veth6787c8d", "state": "blocking"}}

{"source": "kernel", "template_id": 5, "ts": "2026-07-04T20:43:04.596797+00:00",
 "raw_message": "2026-07-05T01:00:50+05:30 fedora kernel: veth51faac0: renamed from eth0",
 "fields": {"timestamp": "2026-07-05T01:00:50+05:30", "hostname": "fedora", "process": "kernel",
            "new_interface": "veth51faac0", "old_interface": "eth0"}}

{"source": "systemd-boot", "template_id": 8, "ts": "2026-07-04T20:48:38.401583+00:00",
 "raw_message": "2026-07-05T04:41:39+05:30 fedora systemd[1]: Starting dracut-pre-udev.service - dracut pre-udev hook...",
 "fields": {"timestamp": "2026-07-05T04:41:39+05:30", "hostname": "fedora", "process": "systemd", "pid": "1",
            "service": "dracut-pre-udev.service", "description": "dracut pre-udev hook"}}

{"source": "systemd-boot", "template_id": 21, "ts": "2026-07-04T20:48:38.401583+00:00",
 "raw_message": "2026-07-05T04:41:46+05:30 fedora warp-svc[1099]: 2026-07-04T23:11:46.083Z DEBUG main_loop:handle_command: warp_connection::controller: Stopping WarpConnection state=Disconnected",
 "fields": {"syslog_timestamp": "2026-07-05T04:41:46+05:30", "hostname": "fedora", "service": "warp-svc",
            "pid": "1099", "app_timestamp": "2026-07-04T23:11:46.083Z", "level": "DEBUG",
            "context": "main_loop:handle_command",
            "message": "warp_connection::controller: Stopping WarpConnection state=Disconnected"}}

{"source": "myapp2", "template_id": 3, "ts": "2026-07-04T20:41:09.881554+00:00",
 "raw_message": "2026-07-05T10:00:28Z myapp WARN user=judy action=login status=failed latency_ms=190",
 "fields": {"timestamp": "2026-07-05T10:00:28Z", "app": "myapp", "level": "WARN", "user": "judy",
            "action": "login", "status": "failed", "latency_ms": "190"}}
```

Note template_id 21 shows the LLM correctly handling a nested-timestamp
format (a syslog-style prefix wrapping an application's own internal ISO
timestamp) by extracting both as separate fields rather than collapsing them.

These `refined.jobs` messages are sitting in RabbitMQ waiting for Phase 5's
`refined-loader` to upsert them into the `logs` table.
