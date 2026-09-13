#!/usr/bin/env -S uv run --script
"""Build and verify exactly the production SDK archives, without acceptance code."""

import argparse
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import tomllib


PACKAGES = (
    "zhir-core", "zhir-policies", "zhir-kernel", "zhir-tools",
    "zhir-builtins", "zhir-models", "zhir-storage", "zhir",
)
EXCLUDED = {"zhir-testing", "zhir-conformance"}
ROOT = Path(__file__).resolve().parent.parent


def verify_archive(path: Path) -> None:
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            parts = Path(member.name).parts[1:]
            if not parts:
                continue
            if parts[0] in {"tests", "benches", "fixtures", "test-results"}:
                raise RuntimeError(f"acceptance file in {path.name}: {member.name}")
            if parts == ("Cargo.toml",):
                manifest = tomllib.loads(archive.extractfile(member).read().decode())
                for section in ("dependencies", "dev-dependencies", "build-dependencies"):
                    forbidden = EXCLUDED.intersection(manifest.get(section, {}))
                    if forbidden:
                        raise RuntimeError(f"test dependencies in {path.name}: {forbidden}")
            if parts == ("Cargo.lock",):
                lock = tomllib.loads(archive.extractfile(member).read().decode())
                forbidden = EXCLUDED.intersection(p["name"] for p in lock["package"])
                if forbidden:
                    raise RuntimeError(f"test packages in {path.name} lockfile: {forbidden}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"], cwd=ROOT,
    ))
    publishable = {p["name"] for p in metadata["packages"] if p["publish"] != []}
    if publishable != set(PACKAGES):
        raise RuntimeError(f"release allowlist differs from publishable packages: {publishable}")
    output = ROOT / "test-results" / "package"
    output.mkdir(parents=True, exist_ok=True)
    # A fresh registry/cache directory prevents reusing earlier sources at the same version.
    with tempfile.TemporaryDirectory(prefix="verify-", dir=output) as target:
        command = ["cargo", "package", "--allow-dirty", "--locked", "--target-dir", target]
        for package in PACKAGES:
            command.extend(["-p", package])
        if args.offline:
            command.append("--offline")
        subprocess.run(command, cwd=ROOT, check=True)
        archives = list((Path(target) / "package").glob("*.crate"))
        if len(archives) != len(PACKAGES):
            raise RuntimeError(f"expected {len(PACKAGES)} archives, found {len(archives)}")
        for archive in archives:
            verify_archive(archive)
            print(f"PASS {archive.name}: compiled archive, no acceptance files or dependencies")


if __name__ == "__main__":
    main()
