# Gzip fixtures

Member streams for streaming decompression and nested byte layers. Prepared
with Python `gzip.compress(..., mtime=0)` and explicit byte changes for invalid
cases. UTF-8 decoded text and member boundaries are part of the expectations.

| File | Case / decoded bytes |
| --- | --- |
| `good_multi_member.gz` | Concatenated `first`, empty, and UTF-8 `第二行` members. |
| `empty_member.gz` | Valid gzip member containing zero decoded bytes. |
| `payload.gz` | Valid member containing `payload`. |
| `nested.gz.gz` | Two nested gzip layers containing `nested`. |
| `bad_empty.gz` | Zero-byte input, without a gzip header. |
| `bad_magic.gz` | Plain text with an invalid gzip signature. |
| `bad_header.gz` | First three bytes of `payload.gz`. |
| `bad_truncated.gz` | `payload.gz` with its final byte removed. |
| `bad_checksum.gz` | `payload.gz` with one CRC bit flipped. |
| `bad_trailing.gz` | Valid `payload.gz` followed by `junk`. |
| `bad_truncated_member.gz` | Valid member followed by a truncated second member. |
| `bad_late_member.gz` | Member containing `ab`, followed by an invalid next member; tests lazy decoding. |

## Use these inputs

Facade feature: `decompression-gzip`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-decompression --no-default-features --features gzip
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
