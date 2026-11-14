# ZIP fixtures

Local archive samples for member selection, sessions, limits, and CRC/length
validation. Corrupt archives are independent committed inputs.

| Fixture | Contents |
| --- | --- |
| `mixed.zip` | Stored padding, Deflate XML, binary PDF bytes, empty file, directory, duplicate name, relative name, compressible text. The 256 KiB padding proves selected reads do not collect the archive. |
| `descriptors.zip` | Stored and Deflate entries written to a non-seekable output, with data descriptors. |
| `zip64.zip` | ZIP64 local header plus ZIP64 end record and locator, with a tiny payload. |
| `bad_zip64_entry_count.zip` | `zip64.zip` with both ZIP64 entry counts set to u64::MAX; rejects capacity overflow before allocation. |
| `bad_zip64_directory_count.zip` | `zip64.zip` with both ZIP64 entry counts set to 5; five minimum 46-byte central-directory headers cannot fit this 217-byte archive. |
| `empty.zip` | Valid empty central directory. |
| `unsupported.zip` | Bzip2 ZIP entry, outside this feature's Stored/Deflate scope. |
| `bad-offset.zip` | Incomplete ZIP64 offset metadata; exercises the upstream parser's missing-offset panic. |
| `bad_magic.zip` | The nine bytes `not a ZIP`; invalid archive signature. |
| `bad_truncated.zip` | `mixed.zip` with its last ten bytes removed. |
| `bad_encrypted.zip` | `mixed.zip` with bit 0 set in the local-header flags of `empty.txt`; even an empty encrypted entry must be rejected. |
| `bad_crc.zip` | `mixed.zip` with the first byte of the stored `%PDF-` payload XORed with 1; forces a terminal CRC mismatch. |
| `bad_expansion_size.zip` | `mixed.zip` with `expanded.txt`'s uncompressed size set to little-endian u32 value 1 in both local and central headers; actual expansion must still obey the limit. |

## Use these inputs

Facade feature: `decompression-zip`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-decompression --no-default-features --features zip
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
