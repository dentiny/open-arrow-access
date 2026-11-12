# WARC fixtures

Small WARC/1.1 physical-record inputs for native and Arrow readers. They cover
binary bodies, fragmentation, lazy failures, and independent reader ownership.
CRLF framing and intentional invalid bytes must remain exact.

| File | Case |
| --- | --- |
| `good_physical_records.warc` | Ordered warcinfo, resource, and continuation records, including an empty body, binary bytes `00 ff 0d 0a`, optional headers, and segment metadata. |
| `good_resource.warc` | First record of `good_resources.warc`; its byte length identifies the fault-injection boundary. |
| `good_resources.warc` | Two complete resource records for lazy reading and errors after the first delivered record. |
| `good_metadata.warc` | One metadata record for independent-reader checks. |
| `empty.warc` | Zero-byte input. |
| `bad_incomplete_header.warc` | Input ends in the middle of a header. |
| `bad_truncated_body.warc` | Five-byte body ends after four bytes. |
| `bad_terminator.warc` | Invalid final record-terminator byte. |
| `bad_missing_date.warc` | Missing required WARC-Date header. |

## Use these inputs

Facade feature: `format-warc`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features warc
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
