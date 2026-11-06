# Bzip2 fixtures

Fixed streams read by `decompression/tests/bz2.rs`; tests do not compress,
concatenate, truncate, or corrupt format input at runtime.

| File | Case / decoded bytes |
| --- | --- |
| `good_members.bz2` | Concatenated `first\n`, empty, and UTF-8 `第二行\n` members. |
| `empty_member.bz2` | Valid member containing zero decoded bytes. |
| `payload.bz2` | Valid member containing `payload`. |
| `bad_empty.bz2` | Zero-byte input without a member header. |
| `bad_magic.bz2` | Plain text with an invalid bzip2 signature. |
| `bad_header.bz2` | First three bytes of the payload member. |
| `bad_truncated.bz2` | Payload member with its final byte removed. |
| `bad_block_checksum.bz2` | Payload member with one block-CRC bit flipped. |
| `bad_stream_checksum.bz2` | Payload member with one combined-stream-CRC bit flipped. |
| `bad_trailing.bz2` | Payload member followed by invalid trailing bytes. |
| `bad_truncated_member.bz2` | Payload member followed by a truncated second member. |

Prepared once with Python's `bz2.compress(..., compresslevel=1)` and explicit
byte mutations for malformed inputs. Preserve exact bytes; `.gitattributes`
disables line-ending conversion. Memory accessors still exercise fragmented
input and injected I/O errors while reading these files.
