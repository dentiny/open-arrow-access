# ZIP fixtures

| Fixture | Contents |
| --- | --- |
| `mixed.zip` | Stored padding, Deflate XML, binary PDF bytes, empty file, directory, duplicate name, relative name, compressible text. The 256 KiB padding proves selected reads do not collect the archive. |
| `descriptors.zip` | Stored and Deflate entries written to a non-seekable output, with data descriptors. |
| `zip64.zip` | ZIP64 local header plus ZIP64 end record and locator, with a tiny payload. |
| `empty.zip` | Valid empty central directory. |
| `unsupported.zip` | Bzip2 ZIP entry, outside this feature's Stored/Deflate scope. |
| `bad-offset.zip` | Incomplete ZIP64 offset metadata; exercises the upstream parser's missing-offset panic. |
| `bad_magic.zip` | The nine bytes `not a ZIP`; invalid archive signature. |
| `bad_truncated.zip` | `mixed.zip` with its last ten bytes removed. |
| `bad_encrypted.zip` | `mixed.zip` with bit 0 set in the local-header flags of `empty.txt`; even an empty encrypted entry must be rejected. |
| `bad_crc.zip` | `mixed.zip` with the first byte of the stored `%PDF-` payload XORed with 1; forces a terminal CRC mismatch. |
| `bad_expansion_size.zip` | `mixed.zip` with `expanded.txt`'s uncompressed size set to little-endian u32 value 1 in both local and central headers; actual expansion must still obey the limit. |

All malformed archives are committed inputs. Tests read them directly without
mutating a valid archive, so each corrupt case can be inspected independently.
