#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["websockets>=16,<17", "python-socks>=2.8,<3"]
# ///
"""Diagnose MiniMax Ogg/Opus completeness; exit success means evidence was collected."""
import argparse
import asyncio
import json
from pathlib import Path
import struct

import websockets
from minimax_tts import subscription


def pages(data):
    result, offset = [], 0
    while offset < len(data):
        assert data[offset:offset + 4] == b"OggS", "invalid Ogg page boundary"
        count = data[offset + 26]
        size = 27 + count + sum(data[offset + 27:offset + 27 + count])
        assert offset + size <= len(data), "truncated Ogg page"
        result.append({"flags": data[offset + 5],
                       "granule": struct.unpack_from("<Q", data, offset + 6)[0],
                       "sequence": struct.unpack_from("<I", data, offset + 18)[0],
                       "bytes": size})
        offset += size
    return result


async def probe(key, endpoint, output, name, text, rate, continuous):
    request = {"event": "task_start", "model": "speech-2.8-hd",
               "voice_setting": {"voice_id": "male-qn-qingse", "speed": 1, "vol": 1, "pitch": 0},
               "audio_setting": {"format": "opus", "sample_rate": rate, "channel": 1},
               "continuous_sound": continuous}
    events, streams = [], []
    async with asyncio.timeout(45):
        async with websockets.connect(endpoint, additional_headers={"Authorization": "Bearer " + key}) as ws:
            events.append(json.loads(await ws.recv()))
            await ws.send(json.dumps(request))
            async for raw in ws:
                event = json.loads(raw)
                if event.get("event") == "sentence_start":
                    streams.append(bytearray())
                data = event.get("data", {})
                if data.get("audio"):
                    chunk = bytes.fromhex(data["audio"])
                    assert streams, "audio without sentence_start"
                    streams[-1].extend(chunk)
                    data["audio"] = {"byte_length": len(chunk)}
                events.append(event)
                if event.get("base_resp", {}).get("status_code", 0):
                    break
                if event.get("event") == "task_started":
                    await ws.send(json.dumps({"event": "task_continue", "text": text}))
                    await ws.send(json.dumps({"event": "task_finish"}))
                if event.get("event") == "task_finished":
                    break
    evidence = []
    for index, data in enumerate(streams):
        file = f"{name}-{index}.ogg"
        (output / file).write_bytes(data)
        framing = pages(data)
        evidence.append({"file": file, "pages": framing,
                         "has_end_page": any(page["flags"] & 4 for page in framing),
                         "max_granule_ms": max((page["granule"] for page in framing), default=0) / 48})
    (output / f"{name}.json").write_text(json.dumps(
        {"request": request, "text": text, "events": events, "streams": evidence},
        ensure_ascii=False, indent=2))
    print(json.dumps({"case": name, "streams": evidence, "terminal": events[-1]}, ensure_ascii=False), flush=True)


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path.home() / ".cc-switch/cc-switch.db")
    parser.add_argument("--provider", default="MiniMax")
    parser.add_argument("--output", type=Path, required=True, help="Fresh evidence directory")
    args = parser.parse_args()
    key, endpoint = subscription(args.database, args.provider)
    args.output.mkdir(parents=True, exist_ok=False)
    for index, case in enumerate([
        ("short", "你好。这是会话测试。请听下一句。", 24000, False),
        ("long", "这是一段音频格式测试。", 24000, False),
        ("continuous", "你好。这是会话测试。请听下一句。", 24000, True),
        ("rate32k", "这是一段音频格式测试。", 32000, False),
    ]):
        if index:
            await asyncio.sleep(15)
        await probe(key, endpoint, args.output, *case)


if __name__ == "__main__":
    asyncio.run(main())
