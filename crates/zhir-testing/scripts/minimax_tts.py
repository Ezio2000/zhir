#!/usr/bin/env -S uv run --script
"""Run the explicit live TTS test using cc-switch, without persisting its key."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import urllib.request
from urllib.parse import urlsplit


def subscription(database_path: Path, provider: str) -> tuple[str, str]:
    with sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True) as database:
        rows = database.execute(
            "SELECT settings_config FROM providers WHERE app_type=? AND name=?",
            ("claude", provider),
        ).fetchall()
    if len(rows) != 1:
        raise SystemExit("Expected exactly one matching cc-switch Claude provider")
    config = json.loads(rows[0][0])["env"]
    key = config["ANTHROPIC_AUTH_TOKEN"]
    if not key.startswith("sk-cp-"):
        raise SystemExit("Expected a Token Plan subscription key; refusing a pay-as-you-go key")
    host = urlsplit(config["ANTHROPIC_BASE_URL"]).hostname
    if host not in {"api.minimaxi.com", "api.minimax.cn", "api.minimax.io"}:
        raise SystemExit("The selected provider must point directly to an official MiniMax API")
    return key, f"wss://{host}/ws/v1/t2a_v2_bidi"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path.home() / ".cc-switch/cc-switch.db")
    parser.add_argument("--provider", default="MiniMax")
    args = parser.parse_args()
    key, endpoint = subscription(args.database, args.provider)
    environment = os.environ.copy()
    environment["MINIMAX_API_KEY"] = key
    environment["MINIMAX_TTS_URL"] = endpoint
    # Cargo does not read macOS system proxy settings. Keep changes process-local.
    for scheme, proxy in urllib.request.getproxies().items():
        if scheme in {"http", "https"}:
            environment.setdefault(scheme.upper() + "_PROXY", proxy)
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        ["cargo", "test", "-p", "zhir-testing", "--no-default-features", "--features", "minimax", "--locked",
         "--test", "minimax_tts", "live_minimax_tts_session", "--", "--ignored", "--exact", "--nocapture"],
        cwd=root, env=environment,
    )
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
