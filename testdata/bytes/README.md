# Byte contract fixtures

Project-authored deterministic binary inputs, shared by the core, storage, and
layer-composition tests. They preserve the original byte offsets and values.

| Fixture | Contents and purpose |
| --- | --- |
| `line_endings.bin` | Project-authored LF, CRLF, a 4096-byte ASCII line plus LF, and a non-UTF-8 final line without a terminator; scalar/SIMD boundary and line-limit checks. |
| `alphabet.bin` | `abcdefghij`, without a newline; seek and range cursors. |
| `data.bin` | `data`, without a newline; capability checks. |
| `input.bin` | `input`, without a newline; document limits and source errors. |
| `binary.bin` | Hex `00 ff 80`; binary document preservation. |
| `empty.bin` | Zero bytes; empty input and generic extension contracts. |
| `decoder.bin` | `abcd`, without a newline; pending chunks and partial consumption. |
| `decoder_invalid.bin` | `x`, without a newline; invalid decoder consumption. |
| `other.bin`, `requested.bin`, `internal_space.bin` | The text `other file`, `requested file`, and `internal space`, without newlines; distinct filesystem objects for whitespace locator tests. |
| `short.bin`, `short_truncated.bin` | `abcdefgh` and `abcd`, without newlines; replace the temporary copy after opening to exercise a short read. |
| `sequence.bin` | Bytes 0 through 63; chunk splitting and bounded I/O concurrency. |
| `large.bin` | 5 MiB + 7 bytes, byte `i` equals `i % 251`; multi-MiB ranges and truncation after open. |
| `gaps.bin` | 1 MiB + 2 dots, with `ab` at offset 0, `cd` at 512 KiB, and `ef` at 1 MiB; disjoint range coalescing. |

The large fixtures are repetitive and compress well in Git. Their lengths cross
the backend's I/O boundaries; do not shorten them to reduce the test inputs.
