#!/usr/bin/env python3
"""Alternate independent before/after processes to reduce order and thermal drift."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
from run import run_child

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--before', type=Path, required=True)
p.add_argument('--after', type=Path, required=True)
p.add_argument('--output-prefix', type=Path, required=True)
p.add_argument('--cases', required=True)
p.add_argument('--backends', default='memory')
p.add_argument('--rows', type=int, default=32768)
p.add_argument('--width', type=int, default=128)
p.add_argument('--repeats', type=int, default=5)
p.add_argument('--iterations', type=int, default=7)
p.add_argument('--http-chunk', type=int, default=16384)
args = p.parse_args()
args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
files = {}
try:
    for variant in ['before','after']:
        path = Path(str(args.output_prefix) + f'-{variant}.jsonl')
        files[variant] = path.open('w')
        binary = getattr(args,variant).resolve()
        files[variant].write(json.dumps({'metadata': {'variant': variant,
            'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
            'design':'alternating process pairs; each process has one warm-up',
            'options': {k: str(v) if isinstance(v,Path) else v for k,v in vars(args).items()}}})+'\n')
    for case in args.cases.split(','):
        for backend in args.backends.split(','):
            for repeat in range(args.repeats):
                order = ['before','after'] if repeat % 2 == 0 else ['after','before']
                for position, variant in enumerate(order):
                    command = [str(getattr(args,variant).resolve()),case,backend,*map(str,[args.rows,args.iterations,args.width,65536,1024,1,0,args.http_chunk,0,0])]
                    result = run_child(command)
                    result.update(repeat=repeat,pair_position=position)
                    files[variant].write(json.dumps(result)+'\n');files[variant].flush()
                    print(f"{variant} {case} {backend} {repeat}: {result['median_seconds']*1000:.3f} ms",flush=True)
finally:
    for file in files.values(): file.close()
