# ZIP fixtures

Run `python3 testdata/zip/generate.py` from the repository root to regenerate
these deterministic fixtures using Python's standard library.

| Fixture | Contents |
| --- | --- |
| `mixed.zip` | Stored padding, Deflate XML, binary PDF bytes, empty file, directory, duplicate name, relative name, compressible text. The 256 KiB padding proves selected reads do not collect the archive. |
| `descriptors.zip` | Stored and Deflate entries written to a non-seekable output, with data descriptors. |
| `zip64.zip` | ZIP64 local header plus ZIP64 end record and locator, with a tiny payload. |
| `empty.zip` | Valid empty central directory. |
| `unsupported.zip` | Bzip2 ZIP entry, outside this feature's Stored/Deflate scope. |
| `bad-offset.zip` | Incomplete ZIP64 offset metadata; must return Decode without a parser panic. |

Tests mutate copies to exercise CRC corruption, truncation, encrypted flags,
and false declared sizes without maintaining redundant binary fixtures.
