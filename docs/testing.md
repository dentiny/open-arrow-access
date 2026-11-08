# Test locations

This is the starting point for finding and adding tests across the workspace.
Each crate owns its tests under `<crate>/tests/`. Composition tests live in the
crate whose behavior they exercise. The workspace root has no test directory.

## Find a test

| Area | Test location |
| --- | --- |
| CSV and TSV | [`file-format/tests/csv.rs`](../file-format/tests/csv.rs) |
| JSON and JSONL | [`file-format/tests/json.rs`](../file-format/tests/json.rs) |
| XML | [`file-format/tests/xml.rs`](../file-format/tests/xml.rs) |
| ATIF format | [`file-format/tests/atif.rs`](../file-format/tests/atif.rs) |
| Parquet and byte/record layer composition | [`file-format/tests/parquet.rs`](../file-format/tests/parquet.rs) |
| Directory and file-set behavior | [`dataset/tests/directory.rs`](../dataset/tests/directory.rs) |
| Directory, storage and Parquet composition | [`dataset/tests/directory_parquet.rs`](../dataset/tests/directory_parquet.rs) |
| ATIF and file-set composition | [`dataset/tests/atif.rs`](../dataset/tests/atif.rs) |
| pmc-inventory | [`service/tests/pmc_inventory.rs`](../service/tests/pmc_inventory.rs) |

| pmc | [`service/tests/pmc.rs`](../service/tests/pmc.rs) |
| Hugging Face Parquet datasets | [`service/tests/huggingface.rs`](../service/tests/huggingface.rs) |
| CourtListener bulk snapshots | [`service/tests/courtlistener.rs`](../service/tests/courtlistener.rs) |
| Service filename components | [`service/tests/unit/filename_validation.rs`](../service/tests/unit/filename_validation.rs) |
| Common Crawl configuration | [`service/tests/commoncrawl.rs`](../service/tests/commoncrawl.rs) |
| Common Crawl segments | [`service/tests/commoncrawl_segment.rs`](../service/tests/commoncrawl_segment.rs) |
| Common Crawl WARC inventory | [`service/tests/warc.rs`](../service/tests/warc.rs) |
| WARC parser | [`file-format/tests/warc.rs`](../file-format/tests/warc.rs) |
| OpenDAL storage | [`storage/tests/opendal.rs`](../storage/tests/opendal.rs) |
| Compression codecs | [`decompression/tests/`](../decompression/tests/) |
| ZIP entry decompression | [`decompression/tests/zip.rs`](../decompression/tests/zip.rs) |
| Document accessor | [`core/tests/document.rs`](../core/tests/document.rs) |
| Core reader contract | [`core/tests/stream.rs`](../core/tests/stream.rs) |
| Generic file-format extension protocol | [`file-format/tests/accessor.rs`](../file-format/tests/accessor.rs), also runs with no format features |
| Shared format contract assertions | [`test-support/src/format.rs`](../test-support/src/format.rs), used by format and Parquet suites |
| Generic reader lifecycle and source failures | [`test-support/src/reader.rs`](../test-support/src/reader.rs), shared by Arrow formats, WARC and byte layers |

## Add a test

- Put format behavior in `file-format/tests/<format>.rs`, storage behavior in
  `storage/tests/<backend>.rs`, and provider behavior in `service/tests/<provider>.rs`.
  Use the owning crate's public API.
- Put composition tests in the crate that owns the entry point being tested.
  Import shared contracts from core and concrete adapters from their owning crates.
- When a test genuinely requires private implementation access, keep its file in
  the owning crate's `tests/unit/`. Include it from the library with `#[cfg(test)]`
  and `#[path = "../tests/unit/<name>.rs"]`; Cargo does not discover that nested
  file as a separate integration target. Do not expose production internals just
  for testing, or hide test bodies under `src/`.
- Keep shared byte sources, fixture loading, byte-layer and format contract assertions in
  [`test-support/`](../test-support/). Keep helpers used by only one suite alongside
  that suite, or in its `tests/support/` directory.
- Start fixed-schema sequential formats with `format::assert_fixed_schema_contract`
  and a `FormatCase`. Use `byte_accessor::assert_ranged_read_contract` for ranged
  storage and resolved provider files, and `http::assert_resolution_cases` for
  provider metadata failure tables. See the [runner guide](../test-support/README.md#composed-contract-runners).
  Native output formats can use `reader::assert_error_contract` with a metadata
  snapshot and parser-specific source assertions; Arrow schema/projection checks
  remain in `format`.
  Inferred schemas, native record formats and range-read faults retain their own
  specialized contracts; keep parsed-value and provider policy assertions local.
- Read all serialized test inputs, valid and invalid, from root
  `testdata/<format>/` with `fixture!` or `fixture_path!`. Reuse existing samples;
  add a named fixture and document its provenance and intended failure when a
  case is missing. Do not construct documents, encode shards, or corrupt binary
  formats at runtime. Copy fixtures into temporary directories for filesystem
  isolation. Keep expected values and behavioral controls (chunking, projections,
  I/O fault injection, decoder doubles) in test code. See the
  [`testdata` policy](../testdata/README.md).
- Add `required-features` to each optional integration test target in its crate's
  `Cargo.toml`. The normal no-feature build must still work.
- Add a row to this index when introducing a new suite.

## Run tests

Run the entire workspace with every feature enabled:

```sh
cargo test --workspace --all-features --locked
```

Run a single format suite:

```sh
cargo test -p open-arrow-access-file-format --features csv --test csv --locked
cargo test -p open-arrow-access-file-format --features xml --test xml --locked
```

Run the provider suite:

```sh
cargo test -p open-arrow-access-service --features huggingface --test huggingface --locked
```

Run the storage suite:

```sh
cargo test -p open-arrow-access-storage --locked
```

Plain `cargo test` does not select every workspace member or enable optional
formats. Use the workspace command above for complete coverage.
