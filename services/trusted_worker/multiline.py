import re

# A line starting with one of these is treated as the start of a new
# logical log entry; anything else is folded into the previous entry
# (e.g. stack trace continuation lines, which carry no timestamp of
# their own).
_ENTRY_START_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"),  # ISO 8601 (journald, app logs)
    re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}"),  # classic syslog "Jan  5 01:11:22"
    re.compile(r"^\[\d{4}[/-]\d{2}[/-]\d{2}"),  # bracketed date, e.g. nginx error log
]


def starts_new_entry(line: str) -> bool:
    return any(p.match(line) for p in _ENTRY_START_PATTERNS)


def join_multiline(lines: list[str]) -> list[str]:
    entries: list[str] = []
    for line in lines:
        if not entries or starts_new_entry(line):
            entries.append(line)
        else:
            entries[-1] += "\n" + line
    return entries
