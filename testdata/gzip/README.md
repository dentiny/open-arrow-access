# Gzip fixtures

Fixed gzip byte streams used by `decompression/tests/gzip.rs`. Files are read
from disk during tests; compression and corruption happen only when preparing
these checked-in fixtures.

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

Prepared once using Python's `gzip.compress(..., mtime=0)` and explicit byte
mutations for malformed inputs. Preserve the exact bytes; `.gitattributes`
disables line-ending conversion. Mock byte readers still fragment file contents
and inject I/O failures so tests cover streaming and error propagation.
