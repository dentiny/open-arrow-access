# Zstandard fixtures

Fixed streams read by `decompression/tests/zstd.rs` through mock byte readers.
No compression or corruption is performed during tests.

| File | Case / decoded bytes |
| --- | --- |
| `good_frames.zst` | Concatenated skippable, `first\n`, empty, skippable, UTF-8 `第二行\n`, and empty skippable frames. |
| `empty_frame.zst` | Valid empty compressed frame. |
| `skippable_frame.zst` | Skippable frame containing `metadata`. |
| `payload.zst` | Checksummed frame containing `payload`. |
| `bad_empty.zst` | Zero-byte input, without a frame header. |
| `bad_magic.zst` | Plain text with an invalid frame signature. |
| `bad_header.zst` | First three bytes of the payload frame. |
| `bad_truncated.zst` | Payload frame without its final byte. |
| `bad_checksum.zst` | Payload frame with one checksum bit flipped. |
| `bad_trailing.zst` | Payload frame followed by invalid trailing bytes. |
| `bad_truncated_second.zst` | Payload frame followed by a truncated second frame. |
| `bad_truncated_skippable.zst` | Skippable frame with its final byte removed. |

Prepared once with `zstd` 0.14's streaming encoder at level 1 with checksums,
plus explicit skippable-frame headers and byte mutations for invalid inputs.
Preserve the exact file bytes; `.gitattributes` disables line-ending conversion.
