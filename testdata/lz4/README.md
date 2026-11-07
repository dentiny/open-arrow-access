# LZ4 fixtures

Fixed streams read by `decompression/tests/lz4.rs`; tests do not encode frames,
construct skippable frames, or mutate compressed data at runtime.

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

Prepared once using `lz4_flex` 0.13, with block/content checksums enabled, plus
explicit headers and byte mutations for malformed/unsupported inputs.
Preserve exact bytes; `.gitattributes` disables line-ending conversion.
