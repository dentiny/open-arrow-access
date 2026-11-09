#!/usr/bin/env python3
"""Alternate two prebuilt binaries; aggregate all raw trials without compiling."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--optimized', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cases', nargs='+', required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--rows', type=int, default=20000)
    parser.add_argument('--rounds', type=int, default=15)
    args = parser.parse_args()
    assert args.trials > 0
    binaries = {'baseline': args.baseline.resolve(), 'optimized': args.optimized.resolve()}
    result = {'binaries': {name: {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in binaries.items()}, 'cases': {}}
    runner = Path(__file__).with_name('benchmark.py')
    with tempfile.TemporaryDirectory() as temporary:
        trial_file = Path(temporary) / 'trial.json'
        for case in args.cases:
            variants = result['cases'][case] = {'baseline': [], 'optimized': []}
            for trial in range(args.trials):
                order = ['baseline', 'optimized'] if trial % 2 == 0 else ['optimized', 'baseline']
                for variant in order:
                    subprocess.run([sys.executable, str(runner), '--binary', str(binaries[variant]), '--output', str(trial_file), '--cases', case, '--rows', str(args.rows), '--rounds', str(args.rounds)], check=True, stdout=subprocess.DEVNULL)
                    variants[variant].append(json.loads(trial_file.read_text())['cases'][case])
            medians = {name: statistics.median(sample['seconds'] for trial in trials for sample in trial['samples']) for name, trials in variants.items()}
            print(f'{case}: {medians["baseline"] * 1000:.3f} -> {medians["optimized"] * 1000:.3f} ms', flush=True)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
