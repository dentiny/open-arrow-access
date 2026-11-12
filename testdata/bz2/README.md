# Bzip2 fixtures

Member streams for fragmented decoding, concatenation, corruption, and source
error propagation. Prepared with Python `bz2.compress(..., compresslevel=1)`
and deliberate byte changes for invalid cases.

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

## Use these inputs

Facade feature: `decompression-bz2`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-decompression --no-default-features --features bz2
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
