# LZ4 fixtures

Frame streams prepared with `lz4_flex` 0.13, block/content checksums, explicit
skippable headers, and byte changes for malformed or unsupported cases.

| File | Case / decoded bytes |
| --- | --- |
| `good_frames.lz4` | Skippable, `first\n`, skippable, empty, UTF-8 `第二行\n`, and skippable frames. |
| `empty_frame.lz4` | Valid empty compressed frame. |
| `skippable_frame.lz4` | Skippable frame containing `metadata`. |
| `payload.lz4` | Frame containing `payload`, with block and content checksums. |
| `bad_empty.lz4` | Zero-byte input without a header. |
| `bad_header.lz4` | First three bytes of the payload frame. |
| `bad_truncated.lz4` | Payload frame without its final byte. |
| `bad_checksum.lz4` | Payload frame with one content-checksum bit flipped. |
| `bad_header_checksum.lz4` | Payload frame with a header-checksum bit flipped. |
| `bad_trailing.lz4` | Payload frame followed by invalid bytes. |
| `bad_truncated_second.lz4` | Payload frame followed by a truncated second frame. |
| `bad_raw_block.lz4` | Size-prefixed raw LZ4 block, unsupported by the frame reader. |
| `bad_legacy.lz4` | Legacy LZ4 stream, unsupported by the frame reader. |
| `bad_truncated_skippable.lz4` | Truncated skippable-frame payload. |

## Use these inputs

Facade feature: `decompression-lz4`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-decompression --no-default-features --features lz4
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
