import pathlib


def load_env(path: str = ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    p = pathlib.Path(path)
    if not p.exists():
        return env
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env
