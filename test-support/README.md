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
without depending on a concrete layer crate. `byte_layer::assert_fragmented_output`
runs a layer factory over caller-selected input chunk sizes and checks exact,
bounded output. `byte_layer::assert_decode_error` feeds a malformed fixture one
byte at a time and checks a decode error, source chain, terminal EOF, and no further
upstream polls. Keep codec-specific fixture lists and special cases in their tests.
`byte_layer::assert_terminal_error` also supports individual malformed-input cases.

## Composed contract runners

Start a new fixed-schema, sequential Arrow format with one fixture case:

```rust,ignore
let input = fixture!("my_format", "good.data").await;
let batches = format::assert_fixed_schema_contract(
    |bytes| MyFormatAccessor::new(bytes, expected_schema.clone()),
    format::FormatCase {
        bytes: &input,
        request: ReadRequest::new("records").with_options(read_options),
        schema: expected_schema.clone(),
        rows: 3,
        fault_offsets: &[0, 5],
    },
).await;
// Assert format-specific values in the requested projection's returned batches.
```

The runner combines missing/duplicate projection rejection before byte I/O,
full/requested/zero-column schemas and row counts, terminal EOF, open/read failures,
empty chunks, source causes, and byte-option forwarding. Supply independently
expected schema/rows and reachable fault offsets. Fault cases use the full schema
and a one-row target, preserving the request's object and byte options. The full
schema needs at least one field for missing/duplicate projection checks; a
zero-column output projection is supported. Fieldless schemas can use the
individual helpers. CSV, JSON and ATIF use this entry point. Inferred schemas retain their own projection-validation timing tests;
Parquet retains its range-failure tests. The individual helpers remain available
for those inputs and for native record formats such as WARC.

For a ranged backend, `byte_accessor::assert_ranged_read_contract` combines exact
single/bulk ranges and sequential-cursor preservation with independent-reader
ownership, stable metadata and EOF checks. Pass fixture-appropriate ranges; include
duplicates and empty ranges when useful. Filesystem, Zenodo and Internet Archive
use this runner. Sequential accessors use `assert_read_contract` directly.

For providers with one metadata response, `http::assert_resolution_cases` accepts
the calling manifest directory, fixture directory, metadata route, a table of
`(fixture_filename, HTTP_status, expected_error_kind)`, and an async resolver taking
an owned endpoint. Each case gets an isolated `HttpFixture`, loads its committed
response bytes, and requires exactly one metadata request and the expected error.
No attachment routes are installed, so premature downloads fail the test. See the
Zenodo and Internet Archive suites. Keep provider-specific expected values, URL
assertions, successful empty memberships and authentication/pagination cases local.

## Byte accessor contracts

`byte_accessor::assert_read_contract(accessor, request, expected)` opens two
independent readers, advances one cursor, drops the accessor, and checks exact
output, nonempty chunks, known content lengths, stable metadata, and repeated EOF.
It works for sequential and ranged sources without requiring listing support.

`byte_accessor::assert_ranges(reader, expected, ranges)` checks each single range
and the same bulk request, preserving caller order and duplicate/empty ranges.
Use it only when range reads are supported. The caller supplies ranges suited to
its fixture and checks subsequent sequential output to verify cursor preservation.
See `storage/tests/opendal.rs` for filesystem and large-range usage, and the Zenodo
and Internet Archive tests for HTTP-backed provider usage.

Fixed-name namespace rules belong in `storage/tests/named_files.rs`; provider
tests retain membership, URL construction, metadata validation, and one complete
read through their own transport. Backend-specific failures and scheduling
checks stay in the owning test suite.

## Format contracts

`format` shares assertions for single-file Arrow format accessors. The module
depends on core contracts and the memory backend, so adding a format does not
require adding a concrete format dependency to this crate.

Lifecycle and injected source failures are implemented in `reader`, independently
of Arrow batches or a particular request type. The `format` wrappers supply Arrow
metadata snapshots; projection, schema and batch-size assertions stay in `format`.
Native formats can call the same runner directly, as WARC does:

```rust,ignore
reader::assert_error_contract(
    WarcAccessor::new,
    &valid_fixture,
    ByteReadRequest::new("records.warc").with_options(byte_options),
    &[header_offset, body_offset, terminator_offset],
    Clone::clone, // ObjectRef metadata; snapshot all fields whose stability matters.
    assert_warc_source_error, // Keep parser-specific wrapping checks in the suite.
).await;
```

The request must be cloneable and convertible by reference to `ByteReadRequest`,
which defines the expected forwarded object and byte options. Metadata snapshots
only need `Debug + PartialEq`; neither items nor metadata need those traits.
`reader::assert_finished` checks repeated EOF, stable snapshots and no new upstream
polls after an observed EOF/error. `reader::assert_terminal_error` expects the very
next call to fail; byte-layer helpers retain their drain-until-error behavior.

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

`with_open_failure(kind)` records the attempted request and fails before creating
a reader. Format error contracts exercise this alongside sequential faults.

`with_ranges` enables exact reads. `observations.range_reads` and `ranges` record
all range calls, including failures. `with_range_failure(after, kind)` injects
an error after `after` successful calls independently for each opened reader;
the original I/O cause remains in the source chain. `with_short_ranges` returns
short payloads deliberately. `without_content_length` exercises consumers which
require known lengths. These settings do not change the sequential cursor or
sequential fault injection.

## HTTP fixtures

`http::HttpFixture` isolates hosts and captures methods, paths, and headers for
provider tests. Static responses and files take precedence over `handle`'s dynamic
fallback. The fallback receives the original request, including POST bodies;
provider API validation and pagination stay in the calling fixture.
`http::response` and `http::file_response` share fragmented responses and file
HEAD/range behavior. Use `clear_requests()` between phases and
`CapturedRequest::range()` to inspect captured byte ranges.
