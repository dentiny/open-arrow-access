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
| `position-record-{0,1,2}.warc` | Three project-authored resource records with IDs `<urn:test:0>` through `<urn:test:2>` and five-byte `body0` through `body2` bodies, for source-coordinate/resume assertions. |
| `position-record-{0,1,2}.warc.gz` | Each coordinate record compressed as a separate gzip member. |
| `position-records.warc` | The three coordinate records concatenated without extra bytes. |
| `position-records.warc.gz` | The coordinate records in one gzip member, testing offsets within a frame. |
| `position-record-members.warc.gz` | Three complete gzip members, one per coordinate record, testing stored offsets. |
| `position-split-members.warc.gz` | A whitespace member (`space tab CRLF`), then two gzip members per coordinate record, split at half its byte length. Records start at stored offsets 24, 176, and 328; their second halves cross member boundaries. |
| `bad-truncated-header.warc.gz` | Gzip-compressed `WARC/1.0 CRLF` with its final four trailer bytes removed, for object context on sequential/planned errors. |

These project-authored gzip inputs were prepared offline using Python
`gzip.compress(data, mtime=0)` (default compression level 9). Tests read the
committed bytes rather than encoding or corrupting inputs at runtime.

## Use these inputs

Facade feature: `format-warc`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features warc
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
