# Local performance audit

The paired audit uses upstream main `2dc1ac0`; the final complete baseline
and benchmark branch are refreshed to `e08ae08`. Results keep the snapshots separate.
It reuses the local replay harness from the earlier benchmark proposal (#218),
removes its unmerged storage/streaming dependency, and adds projection, nested
struct, timestamp, single-document ATIF and shared provider HTTP workloads.
Production code and released parser versions are unchanged in this benchmark PR.
See [RESULTS.md](RESULTS.md) for measurements and independent upstream fixes.

## Reproduce

```sh
cargo bench --bench pipeline --all-features --no-run --locked
# Copy the exact executable path Cargo prints; do not use a wildcard.
cp target/release/deps/pipeline-EXACT_HASH ../pipeline-before
python3 benchmarks/run.py --binary ../pipeline-before \
  --output work/baseline.jsonl --rows 20000 --iterations 7 --repeats 1
```

The default matrix runs every case except the expensive `atif-tools` fan-out probe,
across memory, filesystem, HTTP replay and S3 replay. Shared service cases run
only with HTTP. All inputs are deterministic, generated before timing; scans
include opening, inference where applicable, decoding, consumption and dropping
outputs. Row/record counts and byte lengths are asserted. There is one untimed
warmup per child process. Formats use 1,024-row batches and 64-KiB output chunks.
`atif-document` is one trajectory with `--rows` steps; other ATIF cases are JSONL.
`xml-struct` has two nested structs and 32 absent optional attributes, exposing
wide nullable-schema validation overhead. It deliberately represents sparse
schemas; it is not a claim about typical XML documents.

```sh
python3 benchmarks/run.py --binary ../pipeline-before \
  --cases storage-sequential --backends http,s3 --rows 8192 --width 128 \
  --io-chunk 65536 --latency-ms 2 --concurrency 4 \
  --output work/latency.jsonl
```

The executable's positional arguments are case, backend, rows, iterations, width,
output chunk, batch rows, I/O concurrency, I/O chunk (0 means backend default),
HTTP response fragment size, and simulated per-request latency in milliseconds.
`--list` prints cases. `cargo bench --bench pipeline --all-features` runs the
standard cases with memory input (HTTP for shared provider paths). The Python
runner adds process CPU/RSS/I/O counters and explicit backend/configuration control.

## Coverage and interpretation

| Path | Workloads | What to inspect |
| --- | --- | --- |
| CSV, JSON, Parquet | Full and id-only projection | Decoder CPU vs physical column pruning |
| ATIF | JSONL full/projected, one document, timestamps, tool fan-out | Conversion, validation and whole-document memory |
| XML | Full/projected, inference, nested path, sparse nested structs | Parser/converter CPU and validation allocations |
| WARC | Owned physical records | Header/framing CPU and buffered body cost |
| Storage | Sequential, exact whole range, scattered/adjacent bulk ranges | Request/byte amplification, copying, concurrency |
| Shared lines | Arbitrary width/chunk settings | Cross-chunk assembly and polling |
| Provider HTTP | Buffered body, streaming download, JSON metadata | Shared transport and materialization costs |

HTTP and S3 use **in-process protocol replay**, with fake benchmark credentials.
There are no network requests, sockets, TLS or real cloud bandwidth measurements.
Latency is a configurable Tokio timer before each response, not an emulated TCP
connection. `http_response_bytes` counts requested response payload, not wire
bytes, and `http_requests` includes HEAD. Filesystem scans use the warm OS cache;
reported block-I/O counters are process OS accounting, not physical-device bytes.
Memory input uses the instrumented test-support accessor, including its counters.

`storage-scattered` asks for 32 bytes every 4,096 bytes, capped at 4,096 ranges;
`storage-adjacent` asks for consecutive 32-byte ranges. Gap-zero coalescing may
merge adjacent reads but never reads unrequested gaps. Range throughput must use
returned `units`, not whole-object `input_bytes`. Parquet encoded size and schema
complexity differ from text inputs: raw format times are not a format ranking.

The three provider cases cover common HTTP paths; they do not benchmark each
provider's catalogue resolver, real Hugging Face/Xet, or credentialed cloud
services. Compression/archive combinations, cold disk, malformed-input scaling,
production corpora, and all of OpenDAL's other backends remain outside this audit.

## CPU, memory and comparisons

`run.py` uses `wait4` for per-process user/system CPU, peak RSS and block I/O.
Those counters include input construction, runtime startup and warmup. They are
not allocation counts or decoder-only peaks. Copies retained by replay and
memory fixtures contribute to RSS. Small/default workloads can have similar
process RSS despite fewer transient allocations. Timed wall samples exclude
fixture generation and runtime startup.

Build both variants before measuring. Do not run builds/tests/profilers at the
same time as timing trials. Alternate fresh baseline/optimized processes:

```sh
python3 benchmarks/compare.py --before ../pipeline-before --after ../pipeline-after \
  --cases atif,atif-timestamp,xml,xml-struct,csv --rows 20000 --width 128 \
  --iterations 15 --repeats 5 --output-prefix work/paired
```

To build the optimized binary, temporarily add these patches to the root manifest,
rebuild, and preserve the executable; then restore the registry manifest/lockfile.
Exact candidate commits, binary hashes and harness hashes are in the result manifest.
Local path patches are never committed to this repository.

```toml
[patch.crates-io]
atif-arrow = { path = "../atif-arrow/crates/atif-arrow" }
xml-arrow = { path = "../xml-arrow-rs/crates/xml-arrow" }
```

Each optimized case exercises one dependency change; CSV is an unchanged control.
Use process-pair medians and inspect every pair, rather than selecting the best
iteration. Host contention and thermal/frequency variation remain possible.
`profile.py` runs a separate macOS `sample` session: never mix its sampled run into
timing results. No portable speedup or cloud throughput guarantee is implied.

The ATIF confirmation interleaves case pairs across repeats; the main comparison
originally grouped pairs by case. The current comparison runner interleaves cases.
On macOS, CPU sampling may need to run outside a restricted sandbox.

## Per-provider follow-up

The [isolated provider/allocation matrix](matrix/README.md) adds all 23 service
feature families plus arXiv metadata, retained short-read capacity and scatter-body
ownership probes. Its [results](matrix/RESULTS.md) refresh the baseline to main
`29b5687` and document four new independent proposals without production changes
in this umbrella PR.
