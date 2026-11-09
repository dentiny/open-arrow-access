# Latest-main provider and allocation-sensitive benchmarks

This extends the existing format/storage matrix in [benchmarks/README.md](../benchmarks/README.md). Production code was refreshed to upstream main `32e68053d6801932fed3e2ed3e0f50e1eaa33f93`. Results and source provenance are in [the supplemental report](../benchmarks/results/2026-11-09-main32e6805/REPORT.md).

## Run locally

```sh
cargo bench --bench performance --all-features --no-run --locked
# Use the executable path printed by Cargo:
python3 scripts/benchmark.py --binary target/release/deps/performance-HASH --output work/baseline.jsonl
python3 scripts/benchmark.py --binary target/release/deps/performance-HASH --cases atif atif-projected --batch 1 --iterations 2 --output work/small-batches.jsonl
```

The custom benchmark executable takes `CASE ITERATIONS CHUNK_BYTES BATCH_ROWS`. Default chunks are 64 KiB and batches are 1,024 rows. The runner warms each case in a separate process, calibrates scan iterations toward at least 0.5 seconds, and measures five isolated child processes. Calibration retains the requested minimum iteration count and caps it at 100,000; very fast cases can finish below the target. The `named-first` candidate takes about 8 ms for 100,000 listings and should be interpreted as an in-memory early-cancellation measurement.

Each JSONL row includes timed scan seconds and completed units, whole-process user/system CPU, peak RSS, page faults, block I/O and context switches, executable SHA-256 and Git provenance. Format fixtures and filesystem file creation occur outside scan timing. CPU/RSS still include setup and allocator retention: RSS is not live reader heap, and CPU/time ratios are approximate. The source Git revision is accompanied by a dirty flag because the harness is also run against uncommitted independent candidate patches; exact source patches and binary identities are retained with the results.

## Coverage

- All six formats: CSV, JSON, Parquet, XML (explicit mappings and inference), ATIF JSONL and native buffered WARC. Projected scans select one column where supported.
- Storage: warm-cache filesystem, HTTP, Hugging Face repository access, sequential provider downloads, sparse filesystem/HTTP/HF range reads, and 100,000-name manifest listings (full scan and first item).
- Every service feature has a representative path. JSON manifest resolvers cover Zenodo, Dataverse, Internet Archive, Figshare, BHL bulk export, Hathifiles, Federal Register, GovInfo, Library of Congress, OpenStax, Semantic Scholar, Regulations, USPTO and bioRxiv. DOAB, OAPEN and arXiv metadata exercise OAI pages. Memory fixtures exercise PMC metadata, Gutenberg RDF, CourtListener bzip2, MECA, arXiv bulk manifests and Common Crawl path resolution. Hugging Face is exercised as a storage path. This is not exhaustive coverage of every provider API, pagination shape, archive member or authentication mode.

Metadata services use the repository's deterministic in-process HTTP fixture transport. Bulk HTTP/HF/download tests use a lazily generated 64 KiB response frame; the contract-test fixture's default three-byte fragmentation is intentionally excluded from bulk throughput measurements. The separate existing matrix covers real loopback HTTP, injected latency, concurrency, I/O sizes, allocation instrumentation and an optional read-only S3 configuration.

The supplemental fixtures deliberately repeat short payloads. In particular, Parquet is highly compressible and its scan throughput is a best-case control, not a format ranking. ATIF adapter scans contain 4,096 trajectories, while other format scans contain 32,768 records. `input_bytes` describes source file size, not projected bytes transferred; sequential/range calls and the existing detailed I/O matrix must be consulted for I/O interpretation. Metadata fixtures are small, so provider samples measure local resolver overhead, not Internet response time or production manifest scaling.

## Compare native format changes

Each upstream PR includes a `conversion` benchmark and reproduction notes. Build the benchmark on the upstream base with only the benchmark target added, then build the candidate with the same lockfile and toolchain. Alternate the two release executables:

```sh
python3 scripts/compare-benchmarks.py /path/to/before /path/to/after --case atif-native --iterations 3 --output work/atif-comparison.jsonl
```

The comparator excludes warm-up, alternates execution order, measures ten processes per variant, and reports median normalized scan time plus a deterministic bootstrap interval. These intervals summarize local repeated samples; they are not guarantees for another machine or corpus. Native conversion improvements do not establish adapter end-to-end gains until the dependency is released and upgraded.
