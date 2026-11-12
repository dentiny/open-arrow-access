# Byte-reader fixtures

Project-authored deterministic inputs for core, storage, and layer contracts.
Lengths and byte offsets are part of the test expectations. The large files
cross backend I/O boundaries and must retain their sizes.

| Fixture | Contents and purpose |
| --- | --- |
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

## Use these inputs

Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-core
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
