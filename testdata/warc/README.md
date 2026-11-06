# WARC test fixtures

These small, checked-in WARC/1.1 files are the inputs to `tests/warc.rs`.
Tests read them from disk and feed their bytes through the mock accessor to
exercise fragmentation, lazy reading, and injected I/O errors. Tests do not
serialize WARC records or construct malformed inputs at runtime.

| File | Case |
| --- | --- |
| `good_physical_records.warc` | Ordered warcinfo, resource, and continuation records, including an empty body, binary bytes `00 ff 0d 0a`, optional headers, and segment metadata. |
| `good_request_response.warc` | Two physical records for projection and one-row batch checks. |
| `good_batch_budget.warc` | Five bodies of 2, 2, 10, 2, and 2 bytes for batch body-byte limits. |
| `good_resource.warc` | First record of `good_resources.warc`; its byte length identifies the fault-injection boundary. |
| `good_resources.warc` | Two complete resource records for lazy reading and errors after the first delivered batch. |
| `good_metadata.warc` | One metadata record for independent-reader checks. |
| `empty.warc` | Zero-byte input. |
| `bad_incomplete_header.warc` | Input ends in the middle of a header. |
| `bad_truncated_body.warc` | Five-byte body ends after four bytes. |
| `bad_terminator.warc` | Invalid final record-terminator byte. |
| `bad_missing_date.warc` | Missing required WARC-Date header. |
| `bad_segment_number.warc` | Non-numeric WARC-Segment-Number. |

Preserve exact bytes, including CRLF framing and the intentionally invalid
records. The local `.gitattributes` disables line-ending conversion.

```sh
cargo test --locked --features format-warc --test warc
```
