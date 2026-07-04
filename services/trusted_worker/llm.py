import json

import requests

from common import config

SYSTEM_PROMPT = """You are a log parsing assistant. You are given several example log \
lines that all share the same underlying format (only specific values differ between \
them, such as timestamps, IDs, IP addresses, or messages).

Produce a single Python `re`-module regular expression that extracts the variable \
fields consistently from ALL of the given examples.

Rules:
- Use named capture groups: (?P<field_name>...)
- Field names must be valid Python identifiers (snake_case).
- Some examples may span multiple lines (contain literal newlines). If so, include \
inline flags at the very start of the pattern, e.g. (?s), so `.` matches newlines.
- The regex does not need to match the entire line; a partial match via re.search is \
fine, but it must capture every variable field.
- Respond with ONLY a JSON object of this exact shape, no prose, no markdown fences:
{"regex": "<pattern>", "fields": {"<field_name>": "<string|int|float|timestamp>", ...}}
"""


def generate_template(samples: list[str]) -> tuple[str, dict]:
    user_content = "Example log lines:\n" + "\n---\n".join(samples)
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": config.OPENROUTER_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0,
        },
        timeout=30,
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    return parsed["regex"], parsed["fields"]
