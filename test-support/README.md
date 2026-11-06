# Shared test backend

Use `open-arrow-access-test-support` only as a workspace dev-dependency. It depends
on the core contracts, so format and decompression tests share one byte backend
without depending on the facade or a production storage adapter.

Load input bytes from the committed `testdata/` fixtures and pass them to
`MemoryAccessor::new(&bytes, chunk_size)`. The default backend supports sequential
reads and reports content length. Opt into ranges, listing, and version metadata
with `with_ranges`, `with_listing`, and `with_version`.

`with_failure(offset, kind)` injects a sequential failure on the first read whose
cursor reaches or passes the offset. It preserves the selected shared error kind
and attaches a `ConnectionReset` I/O source. `with_empty_chunk(offset)` instead
emits one deliberately invalid empty chunk. The most recent fault builder replaces
any earlier fault configuration. Faults and cursors are independent for each
opened reader; EOF and injected errors terminate sequential consumption.

Accessor clones share `observations`: captured open requests (including options)
and a counter of every `next` call. Assert request forwarding in the calling test,
and compare read counts before and after terminal calls to catch unwanted polling.
Range reads leave sequential cursors unchanged and do not trigger sequential faults.

Use `fixture!(format, name)` to read committed fixtures relative to the calling
crate. `fixture_path` and `read_fixture` also accept an explicit manifest directory.
Use `collect_bytes` to
consume a reader while asserting nonempty chunks. `byte_layer::assert_error_contract`
accepts a layer factory, encoded input, expected output, and error operation. It
checks source categories/causes, terminal reads, and removal of range support
without depending on a concrete layer crate. `byte_layer::assert_terminal_error`
also supports individual malformed-input cases.

Keep record/schema assertions and format-specific scenarios in the owning crate's
tests.
