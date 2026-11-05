# Contiguous range memory

Parquet's `AsyncFileReader` requires contiguous `Bytes` for each range. Joining
fragmented OpenDAL buffers therefore still requires copying. The adapter avoids
retaining a complete fragmented request alongside all contiguous outputs:

- Ranges larger than 2 MiB stream through `Reader::read_into` into one final
  allocation. Input chunks are released as they are copied.
- Smaller ranges use `Reader::fetch` in batches whose requested lengths sum to
  at most 2 MiB. Contiguous buffers keep the existing zero-copy conversion;
  fragmented buffers are joined within that bounded batch.
- I/O chunks default to 2 MiB. Explicit chunk sizes and concurrency still apply,
  and larger settings can increase temporary memory.

Coalescing is limited to each small-range batch. Large ranges and ranges across
batch boundaries are read separately, so overlapping large requests may reread
shared bytes. Large ranges retain concurrent chunk reads. Batches are processed
sequentially to avoid multiplying the per-reader I/O concurrency limit.

The returned contiguous ranges, Parquet row-group data, decoded Arrow arrays,
allocator retention and consumer-held batches still consume memory. These changes
are not a total decoder-memory limit or an end-to-end zero-copy claim.

## Measurement

Measured on arm64 macOS on 2026-11-05 with release builds. The baseline is commit
`3aa33eb`; the comparison uses the streaming/batched implementation above.
Each fixture contains one row group with eight non-null UTF-8 columns, 128 bytes
per value, no dictionary encoding and no page compression. The 131,072-row file
is about 132 MiB; the 262,144-row file is about 264 MiB. Narrow reads select only
`c0`; full reads select all eight columns. Arrow batches contain up to 8,192 rows.

The harness generates fixtures in a separate process. Each read runs in a fresh
process, with a warm-up before five measured trials per case and alternating
baseline/current execution order. Peak RSS comes from `wait4` for that child;
elapsed time covers opening and consuming the read. Results are medians. The
filesystem cache is warm; these are local measurements, not throughput guarantees.

| Rows | Projection | I/O concurrency | Before peak RSS (MiB) | After peak RSS (MiB) | Before (ms) | After (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 131,072 | 1 column | 1 | 42.8 | 28.4 | 4.4 | 3.6 |
| 131,072 | 1 column | 4 | 42.9 | 36.8 | 4.1 | 2.8 |
| 131,072 | 8 columns | 1 | 274.5 | 153.5 | 33.5 | 24.2 |
| 131,072 | 8 columns | 4 | 274.6 | 162.9 | 32.2 | 18.5 |
| 262,144 | 1 column | 1 | 74.9 | 44.9 | 8.2 | 6.6 |
| 262,144 | 1 column | 4 | 75.0 | 53.5 | 7.6 | 4.6 |
| 262,144 | 8 columns | 1 | 539.1 | 285.5 | 67.9 | 52.3 |
| 262,144 | 8 columns | 4 | 539.1 | 292.3 | 65.2 | 35.0 |

Both versions use default I/O chunk options. The baseline leaves chunk splitting
unset; the new adapter explicitly uses 2 MiB chunks. At concurrency four this
also enables parallel chunks within each large range, so the time improvement
includes that scheduling change as well as reduced memory overhead.

## Reproduce

Build the harness and generate a fixture (use an absolute path):

```sh
cargo build --release --bench parquet_memory --all-features --locked
BENCH_BINARY="$(rg --files target/release/deps | rg '/parquet_memory-[0-9a-f]+$')"
"$BENCH_BINARY" generate "$PWD/target/large.parquet" 262144
"$BENCH_BINARY" read "$PWD/target/large.parquet" narrow 1
```

To compare implementations, build the same benchmark source and manifest bench
entry in a separate checkout of baseline `3aa33eb`, and retain that executable
outside its target directory. Then use the current executable and the same fixture:

```sh
python3 benches/measure_parquet_memory.py \
  /absolute/path/to/baseline-binary "$BENCH_BINARY" "$PWD/target/large.parquet" 5
```

The script reports individual samples and medians as JSON on macOS or Linux.
