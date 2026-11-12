# Test support for extensions

Use `open-arrow-access-test-support` as a workspace dev-dependency when adding a
format, byte layer, storage adapter, or service. It provides a memory byte backend
and reusable contract assertions without depending on a concrete production
format or the facade.

```toml
[dev-dependencies]
open-arrow-access-test-support.workspace = true
```

Serialized inputs live in [`testdata/`](../testdata/README.md). Load committed
bytes with `fixture!(directory, name)` or use `fixture_path!` for filesystem
inputs. Keep expected schemas, values, and request options in the owning test.

## Configure a byte source

`MemoryAccessor::new(&bytes, chunk_size)` supplies sequential reads and known
length. Every open owns an independent cursor and terminal state.

| Builder / observation | Use it to check |
| --- | --- |
| `with_ranges`, `with_listing`, `with_version` | Optional operations and source identity |
| `without_content_length`, `with_short_ranges` | Ranged-consumer admission and exact-read validation |
| `with_failure(offset, kind)` | Sequential source error category/cause preservation |
| `with_open_failure(kind)` | Validation/open error paths |
| `with_range_failure(after, kind)` | Failure after a specified number of successful range calls |
| `with_empty_chunk(offset)` | Rejection of invalid empty upstream chunks |
| Shared `observations` | Forwarded requests/options, read counts, and requested ranges |

Sequential faults trigger when the cursor reaches/passes the offset. The latest
sequential fault builder replaces earlier fault settings. Range reads do not
move the sequential cursor or trigger its faults.

<a id="composed-contract-runners"></a>

## Choose a contract runner

| Extension | Start with | Keep in your own suite |
| --- | --- | --- |
| Fixed-schema sequential Arrow format | `format::assert_fixed_schema_contract` | Parsed values, inference behavior, decoder-specific errors |
| Byte layer / codec | `byte_layer::assert_fragmented_output`, `assert_decode_error`, `assert_error_contract` | Valid/malformed fixture selection, codec semantics |
| Sequential byte accessor | `byte_accessor::assert_read_contract` | Backend-specific behavior |
| Ranged byte accessor | `byte_accessor::assert_ranged_read_contract` | Range scheduling and backend failures |
| ZIP/TAR-like ranged consumer | `ranged_input::assert_contract` | Entry parsing, limits, lifetime and cancellation behavior |
| JSON metadata service | `http::assert_resolution_cases` | Identity/version, membership, URLs, auth and pagination |

The format runner checks requested/zero-column projection, fixed metadata,
row counts, terminal EOF/errors, option forwarding, and source-error chains.
Supply independently expected rows/schema and reachable fault offsets. Inferred
formats and ranged formats retain specialized timing/failure tests.

Individual `format` helpers include `collect_batches`, `assert_projection`, and
`assert_invalid_projection_before_open`. Generic `reader` helpers cover terminal
reads and source errors for native record types too. Use `collect_bytes` to
consume byte output while asserting nonempty chunks. `DropProbe` observes release
of streams/readers on EOF, errors, and explicit drop.

## Isolate HTTP tests

`http::HttpFixture` supplies local transport responses and captures methods,
paths, headers, ranges, and request bodies. Static responses/files take priority
over the dynamic `handle` fallback. `http::response` and `file_response` support
fragmentation and file HEAD/range behavior. Use `clear_requests()` between phases
and `CapturedRequest::range()` for range assertions.

Provider tests should verify that metadata resolution does not eagerly download
attachments. Configure only fixture-backed routes, then assert the requests and
selected file metadata. Keep tests offline.

See [testing guidance](../docs/testing.md) and the suites in
[`file-format/tests`](../file-format/tests/), [`decompression/tests`](../decompression/tests/),
and [`service/tests`](../service/tests/) for complete examples.
