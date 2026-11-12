# Zstandard fixtures

Frame streams prepared with `zstd` 0.14's streaming encoder at level 1 with
checksums, explicit skippable-frame headers, and byte changes for invalid cases.

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

## Use these inputs

Facade feature: `decompression-zstd`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-decompression --no-default-features --features zstd
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
