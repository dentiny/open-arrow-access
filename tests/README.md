# Test locations

This is the starting point for finding and adding tests across the workspace.
Each crate owns its behavior tests under `<crate>/tests/`. The root `tests/`
directory owns facade wiring and composition across crates.

## Find a test

| Area | Test location |
| --- | --- |
| CSV and TSV | [`file-format/tests/csv.rs`](../file-format/tests/csv.rs) |
| JSON and JSONL | [`file-format/tests/json.rs`](../file-format/tests/json.rs) |
| XML | [`file-format/tests/xml.rs`](../file-format/tests/xml.rs) |
| ATIF format | [`file-format/tests/atif.rs`](../file-format/tests/atif.rs) |
| Parquet and byte/record layer composition | [`tests/reading.rs`](reading.rs) |
| Directory and file-set facade | [`tests/directory.rs`](directory.rs) |
| ATIF facade and file-set composition | [`tests/atif.rs`](atif.rs) |
| PMC article metadata | [`service/tests/pmc.rs`](../service/tests/pmc.rs), [`tests/pmc.rs`](pmc.rs) |
| WARC parser | [`tests/warc.rs`](warc.rs) |
| OpenDAL storage | [`storage/tests/opendal.rs`](../storage/tests/opendal.rs) |
| Compression codecs | [`decompression/tests/`](../decompression/tests/) |
| Core reader contract | [`core/tests/stream.rs`](../core/tests/stream.rs) |
| Generic file-format extension protocol | [`file-format/tests/accessor.rs`](../file-format/tests/accessor.rs), also runs with no format features |
| Shared memory backend | [`test-support/tests/memory.rs`](../test-support/tests/memory.rs) |
| Shared format contract assertions | [`test-support/src/format.rs`](../test-support/src/format.rs), used by format and Parquet suites |

## Add a test

- Put format behavior in `file-format/tests/<format>.rs`, storage behavior in
  `storage/tests/<backend>.rs`, and provider behavior in `service/tests/<provider>.rs`.
  Use the owning crate's public API.
- Keep facade exports and cross-crate composition in root `tests/<scenario>.rs`.
  Do not duplicate a format's full behavior suite there.
- When a test genuinely requires private implementation access, keep its file in
  the owning crate's `tests/unit/`. Include it from the library with `#[cfg(test)]`
  and `#[path = "../tests/unit/<name>.rs"]`; Cargo does not discover that nested
  file as a separate integration target. Do not expose production internals just
  for testing, or hide test bodies under `src/`.
- Keep shared byte sources, fixture loading, byte-layer and format contract assertions in
  [`test-support/`](../test-support/). Keep helpers used by only one suite alongside
  that suite, or in its `tests/support/` directory.
- Store committed input fixtures in root `testdata/<format>/`. Reuse deterministic
  local fixtures rather than adding network-dependent tests.
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

Run the storage suite:

```sh
cargo test -p open-arrow-access-storage --locked
```

Plain `cargo test` does not select every workspace member or enable optional
formats. Use the workspace command above for complete coverage.
