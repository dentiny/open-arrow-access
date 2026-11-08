# Shared test backend

Use `open-arrow-access-test-support` only as a workspace dev-dependency. It depends
on the core contracts, so format and decompression tests share one byte backend
without depending on the facade or a production storage adapter.

Load input bytes from the committed `testdata/` fixtures and pass them to
`MemoryAccessor::new(&bytes, chunk_size)`. The default backend supports sequential
reads and reports content length. Opt into ranges, listing, and version metadata
with `with_ranges`, `with_listing`, and `with_version`.

This applies to both valid and invalid inputs. Add a documented fixture when a
case is missing rather than constructing or corrupting serialized input in the
test. See the [fixture policy](../testdata/README.md). Expected values and injected
reader behavior belong in test code.

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

## Format contracts

`format` shares assertions for single-file Arrow format accessors. The module
depends on core contracts and the memory backend, so adding a format does not
require adding a concrete format dependency to this crate.

| Helper | Contract |
| --- | --- |
| `collect_batches` | Source identity, stable schema, nonempty batches within the requested row target, and terminal EOF; returns batches for value checks. |
| `assert_projection` | Expected projected schema, caller column order, field/schema metadata and total row count, including zero columns; returns batches. |
| `assert_invalid_projection_before_open` | Missing and duplicate fields fail as `InvalidInput` before any byte open or read. Use with a valid fixed/explicit schema. |
| `assert_finished` | After EOF/error, repeated reads return EOF with stable metadata and no new upstream polls. |
| `assert_terminal_error` | The next read fails with the expected kind and terminates; returns the error for operation/parser checks. |
| `assert_error_contract` | A format factory over `MemoryAccessor` preserves every shared source error category and cause, rejects empty chunks, forwards the object and byte options, and stops polling after read failures. |

Pass the expected full schema and row count to `assert_projection`; the helper
derives expected fields using Arrow's schema API independently of the accessor.
It checks the reader metadata even for input that produces no batches.

For `assert_error_contract`, pass valid fixture bytes, the format's read request,
and reachable fault offsets. It uses one-byte chunks and accepts failures during
either opening/inference or subsequent reads. Supply nondefault byte options in
the request to exercise option forwarding. Parquet's range-read faults remain
in its own suite because the memory backend injects sequential faults only.
Inferred schemas also keep their own projection-validation timing tests.

See `file-format/tests/csv.rs` for a format factory and explicit projections,
`file-format/tests/xml.rs` for inference and explicit-schema cases, and
`file-format/tests/parquet.rs` for shared projection assertions over Parquet.
Keep fixtures, expected schemas, parsed-value checks, parser error context,
read-ahead/drop behavior and format-specific scenarios in the owning tests.

## Ranged sources

`with_ranges` enables exact reads. `observations.range_reads` and `ranges` record
all range calls, including failures. `with_range_failure(after, kind)` injects
an error after `after` successful calls independently for each opened reader;
the original I/O cause remains in the source chain. `with_short_ranges` returns
short payloads deliberately. `without_content_length` exercises consumers which
require known lengths. These settings do not change the sequential cursor or
sequential fault injection.
