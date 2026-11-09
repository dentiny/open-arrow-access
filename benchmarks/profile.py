#!/usr/bin/env python3
"""Collect a macOS CPU sample from a long-running benchmark, then stop the child."""
import argparse
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--case', default='atif-tools')
p.add_argument('--rows', type=int, default=64)
p.add_argument('--width', type=int, default=1024)
args = p.parse_args()
args.output.parent.mkdir(parents=True, exist_ok=True)
child = subprocess.Popen([str(args.binary.resolve()), args.case, 'memory', str(args.rows), '1000000', str(args.width)], stdout=subprocess.DEVNULL)
try:
    time.sleep(1)
    subprocess.run(['sample', str(child.pid), '3', '1', '-file', str(args.output.resolve())], check=True)
finally:
    child.terminate()
    child.wait()
