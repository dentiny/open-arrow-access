#!/usr/bin/env python3
"""Run each provider's public operation against immutable in-process fixtures."""

import argparse
import hashlib
import subprocess
import json
import os
from pathlib import Path
import platform
import random
from run import measure

PROVIDERS = "arxiv bhl biorxiv commoncrawl courtlistener dataverse doab federal-register figshare govinfo gutenberg hathifiles huggingface internet-archive library-of-congress meca oapen openstax pmc pmc-inventory regulations semantic-scholar uspto zenodo".split()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary", default="target/release/open-arrow-access-benchmarks"
    )
    parser.add_argument("--compare-binary")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", required=True)
    parser.add_argument("--providers", default=",".join(PROVIDERS))
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(0)
    with output.open("w") as stream:
        stream.write(
            json.dumps(
                {
                    "type": "environment",
                    "platform": platform.platform(),
                    "arguments": vars(args),
                    "workspace_git_head": subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], text=True
                    ).strip(),
                    "binary_sha256": {
                        str(Path(p).resolve()): hashlib.sha256(
                            Path(p).read_bytes()
                        ).hexdigest()
                        for p in [args.binary, args.compare_binary]
                        if p
                    },
                }
            )
            + "\n"
        )
        for repeat in range(args.repeats):
            cases = [(name, args.binary) for name in args.providers.split(",")]
            if args.compare_binary:
                cases += [
                    (name, args.compare_binary) for name in args.providers.split(",")
                ]
            rng.shuffle(cases)
            for name, binary in cases:
                row = measure(
                    [
                        str(Path(binary).resolve()),
                        "provider",
                        name,
                        str(args.iterations),
                    ],
                    os.environ.copy(),
                )
                row["repeat"] = repeat
                row["binary"] = Path(binary).name
                stream.write(json.dumps(row) + "\n")
                stream.flush()
                print(
                    Path(binary).name,
                    name,
                    f"{row['seconds'] / args.iterations * 1e6:.1f} us/op",
                    flush=True,
                )


if __name__ == "__main__":
    main()
