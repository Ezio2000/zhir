#!/usr/bin/env -S uv run --script
"""Encode a short spoken file as paced 20 ms Opus packets for the Live integration test."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, default=Path("test-results/native-support/input-packets.json"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zhir-live-input-") as directory:
        encoded = Path(directory) / "input.ogg"
        subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-i", str(args.source),
                        "-ar", "48000", "-ac", "2", "-c:a", "libopus", "-frame_duration", "20", str(encoded)], check=True)
        data = encoded.read_bytes()
    offset = 0
    packet = bytearray()
    packets = []
    while offset < len(data):
        if data[offset:offset + 4] != b"OggS" or offset + 27 > len(data):
            raise ValueError("invalid Ogg page")
        count = data[offset + 26]
        lengths = data[offset + 27:offset + 27 + count]
        offset += 27 + count
        for size in lengths:
            packet.extend(data[offset:offset + size])
            offset += size
            if size < 255:
                if not packet.startswith((b"OpusHead", b"OpusTags")):
                    packets.append(list(packet))
                packet.clear()
    if packet or not packets or len(packets) >= 125:
        raise ValueError("provide a complete spoken fixture shorter than 2.5 seconds")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(packets))
    print(f"{len(packets)} Opus packets ({len(packets) * 20} ms): {args.output}")


if __name__ == "__main__":
    main()
