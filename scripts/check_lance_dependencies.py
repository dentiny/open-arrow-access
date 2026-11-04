"""Check resolved dependency families against the recorded Lance baseline.

Run locally with Python 3 and Cargo. This is an offline check of the recorded
revision, not a network lookup of Lance's moving main branch.
"""

import json
from pathlib import Path
import subprocess
import sys


def main():
    """Resolve all enabled implementations and reject dependency version drift."""
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            "cargo", "metadata", "--format-version", "1", "--all-features",
            "--locked", "--offline", "--manifest-path", str(root / "Cargo.toml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)
    baseline = metadata["metadata"]["lance"]
    versions = baseline["versions"]
    seen = set()
    failures = []
    for package in metadata["packages"]:
        name = package["name"]
        family = name.split("-", 1)[0]
        if family not in versions:
            continue
        seen.add(family)
        if package["version"] != versions[family]:
            failures.append(
                f'{name}: {package["version"]}, expected {versions[family]}'
            )
        if package["source"] != "registry+https://github.com/rust-lang/crates.io-index":
            failures.append(f'{name}: unexpected source {package["source"]}')
    if failures:
        print("Lance dependency alignment failed:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(f'Lance baseline: {baseline["revision"]}')
    for family, version in sorted(versions.items()):
        status = "resolved" if family in seen else "reserved; not in dependency graph"
        print(f"  {family}: {version} ({status})")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr, end="")
        sys.exit(error.returncode)
