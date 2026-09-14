#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["websockets>=16,<17", "python-socks>=2.8,<3"]
# ///
"""Probe Codex Live controls, call lifetime and public primary access using real OAuth.

Exit success means the probe completed, not that interruption or recovery is supported.
The JSON evidence records provider rejections alongside successful sideband joins.
"""
import argparse
import asyncio
import base64
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import websockets

ROOT = Path(__file__).resolve().parents[3]


class Probe:
    def __init__(self, args, mode):
        self.args, self.mode = args, mode
        self.directory = args.output / mode
        self.directory.mkdir(parents=True, exist_ok=False)
        tokens = json.loads(args.auth.read_text())["tokens"]
        self.headers = {"Authorization": "Bearer " + tokens["access_token"],
                        "ChatGPT-Account-Id": tokens["account_id"],
                        "OpenAI-Alpha": "quicksilver=v2", "originator": "codex_cli_rs"}
        self.events, self.sockets, self.readers = [], [], []
        self.began = time.monotonic()

    def record(self, kind, **fields):
        self.events.append({"ms": round((time.monotonic() - self.began) * 1000),
                            "kind": kind, **fields})
        (self.directory / "evidence.json").write_text(
            json.dumps(self.events, ensure_ascii=False, indent=2))
        if kind != "event":
            print(self.mode, kind, json.dumps(fields, ensure_ascii=False), flush=True)

    async def connect(self, url, label):
        try:
            ws = await websockets.connect(url, additional_headers=self.headers,
                                          proxy=self.args.proxy, open_timeout=12,
                                          close_timeout=2, max_queue=1024)
        except websockets.exceptions.InvalidStatus as error:
            self.record("rejected", label=label, status=error.response.status_code,
                        body=bytes(error.response.body).decode(errors="replace"))
            return None
        self.sockets.append(ws)
        self.record("attached", label=label, status=ws.response.status_code)
        self.readers.append(asyncio.create_task(self.read(ws, label)))
        return ws

    async def read(self, ws, label):
        try:
            async for raw in ws:
                event = json.loads(raw)
                # PCM observer frames are large and have no generation or replay cursor.
                # Primary RTP metadata is recorded separately by the Rust process.
                if event["type"] not in ("session.input_audio.append", "session.output_audio.delta"):
                    self.record("event", label=label, event=event)
        except websockets.exceptions.ConnectionClosed as error:
            self.record("socket_closed", label=label, code=error.rcvd.code if error.rcvd else None)

    async def wait_for(self, predicate, timeout=15):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(.05)

    async def run_primary(self):
        """Check primary WebSocket startup separately from existing-call sideband access."""
        try:
            for model, voice in (("gpt-live-1-codex", "cove"), ("gpt-live-1", "marin")):
                url = "wss://api.openai.com/v1/live/sessions"
                self.record("primary_access_requested", label=model, url=url,
                            credential_kind="codex_oauth")
                ws = await self.connect(url, model)
                if ws is None:
                    continue
                session = {"model": model, "audio": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "output": {"voice": voice}},
                    "instructions": "You are a brief voice test assistant.",
                    "delegation": {"type": "client"}}
                await ws.send(json.dumps({"type": "session.start", "session": session}))

                def observed(event_type):
                    return any(e.get("label") == model
                               and e.get("event", {}).get("type") == event_type
                               for e in self.events)

                await self.wait_for(lambda: observed("session.started") or observed("error"))
                if observed("session.started"):
                    await ws.send(json.dumps({"type": "session.close"}))
                    await self.wait_for(lambda: observed("session.closed") or observed("error"))
                self.record("primary_access_result", label=model,
                            started=observed("session.started"),
                            finalized=observed("session.closed"), rejected=observed("error"))
                await ws.close()
            self.record("probe_completed", interruption_verified=False, recovery_verified=False)
        finally:
            for ws in self.sockets:
                await ws.close()
            await asyncio.gather(*self.readers)

    async def run(self):
        environment = os.environ.copy()
        environment.update(ZHIR_PROBE_DIR=str(self.directory),
                           ZHIR_LIVE_AUTH_JSON=str(self.args.auth),
                           ZHIR_LIVE_PROXY=self.args.proxy,
                           ZHIR_LIVE_INPUT_PACKETS=str(self.args.input))
        log = (self.directory / "primary.log").open("w")
        process = subprocess.Popen(
            ["cargo", "test", "-p", "zhir-testing", "--features", "openai-live",
             "--test", "live_subscription_probe", "--locked", "--", "--ignored", "--nocapture"],
            cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT)
        try:
            await self.wait_for(lambda: (self.directory / "started.json").exists()
                                or process.poll() is not None, timeout=120)
            if not (self.directory / "started.json").exists():
                raise RuntimeError(f"primary startup failed; inspect {self.directory / 'primary.log'}")
            created = json.loads((self.directory / "created.json").read_text())
            started = json.loads((self.directory / "started.json").read_text())
            call_id = created["location"].rstrip("/").rsplit("/", 1)[-1]
            assert call_id == started["session"]["id"]
            self.record("primary_started", creation=created, session=started["session"])
            url = "wss://api.openai.com/v1/live/" + call_id + "?graceful_close=true"
            ws = await self.connect(url, "primary-alive")
            assert ws is not None
            await ws.close()
            ws = await self.connect(url, "sideband-rejoin")
            assert ws is not None
            await ws.send(json.dumps({"type": "input_audio.pause", "event_id": "probe-pause"}))
            await ws.send(json.dumps({"type": "input_audio.resume", "event_id": "probe-resume"}))
            await self.wait_for(lambda: any(e.get("event", {}).get("type") == "input_audio.resumed"
                                           for e in self.events))
            (self.directory / "speak").touch()
            await self.wait_for(lambda: any(e.get("event", {}).get("type") == "output_transcript.added"
                                           for e in self.events))
            commands = [{"type": "response.cancel"}, {"type": "output_audio_buffer.clear"},
                        {"type": "output_audio.playback.play", "audio": base64.b64encode(bytes(960)).decode()}]
            for index, command in enumerate(commands):
                command["event_id"] = f"probe-output-{index}"
                (self.directory / f"command-{index}.json").write_text(json.dumps(command))
                self.record("output_control_sent", command=command["type"], event_id=command["event_id"])
                await asyncio.sleep(1)
            await self.wait_for(lambda: all(any(e.get("event", {}).get("error", {}).get("event_id") == c["event_id"]
                                               for e in self.events) for c in commands))
            if self.mode == "kill":
                pid = int((self.directory / "pid").read_text())
                os.kill(pid, signal.SIGKILL)
                self.record("primary_sigkill", pid=pid)
            else:
                (self.directory / "destroy").touch()
                await self.wait_for(lambda: (self.directory / "destroyed.json").exists())
                self.record("primary_destroyed", result=json.loads((self.directory / "destroyed.json").read_text()))
            await self.wait_for(lambda: process.poll() is not None)
            self.record("primary_process_exited", exit_code=process.returncode)
            joined = await self.connect(url, "after-primary-exit")
            if joined:
                for event_type in ("input_audio.pause", "input_audio.resume", "input_audio.append", "session.input_audio.append"):
                    command = {"type": event_type, "event_id": "after-exit-" + event_type}
                    if event_type.endswith("append"):
                        command["audio"] = base64.b64encode(bytes(960)).decode()
                    await joined.send(json.dumps(command))
                await asyncio.sleep(20)
                await joined.close()
            await ws.close()
            await asyncio.sleep(3)
            last = await self.connect(url, "after-primary-exit-later")
            if last:
                await last.send(json.dumps({"type": "session.close"}))
                await self.wait_for(lambda: any(e.get("event", {}).get("type") == "session.closed"
                                               for e in self.events))
                await last.close()
            fork = await self.connect("wss://api.openai.com/v1/live/sessions/" + call_id + "/fork", "fork")
            if fork:
                await fork.send(json.dumps({"type": "session.start", "session": {}}))
                await self.wait_for(lambda: any(e.get("label") == "fork" and e.get("kind") == "event"
                                               for e in self.events))
                await fork.close()
            self.record("probe_completed", interruption_implemented=False, recovery_implemented=False)
        finally:
            (self.directory / "destroy").touch()
            for ws in self.sockets:
                await ws.close()
            await asyncio.gather(*self.readers)
            if process.poll() is None:
                process.terminate()
                await asyncio.to_thread(process.wait, timeout=5)
            log.close()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="JSON 20 ms Opus speech packets (required for close/kill/both)")
    parser.add_argument("--auth", type=Path, default=Path.home() / ".codex/auth.json")
    parser.add_argument("--proxy", default=os.environ.get("HTTPS_PROXY"))
    parser.add_argument("--output", type=Path, required=True, help="Fresh evidence directory")
    parser.add_argument("--mode", choices=["primary", "close", "kill", "both"], default="both")
    args = parser.parse_args()
    args.output, args.auth = args.output.resolve(), args.auth.resolve()
    if args.input:
        args.input = args.input.resolve()
    if args.mode != "primary" and args.input is None:
        parser.error("--input is required for close/kill/both")
    if not args.proxy:
        parser.error("provide --proxy or HTTPS_PROXY for the subscription signaling route")
    if args.mode == "primary":
        await Probe(args, "primary").run_primary()
        return
    for mode in (["close", "kill"] if args.mode == "both" else [args.mode]):
        await Probe(args, mode).run()


if __name__ == "__main__":
    asyncio.run(main())
