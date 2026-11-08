"""Regenerate deterministic ZIP fixtures using only Python's standard library."""
import io
from pathlib import Path
import struct
import warnings
import zipfile

ROOT = Path(__file__).parent


def write(archive, name, data, compression=zipfile.ZIP_STORED):
    info = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
    info.compress_type = compression
    archive.writestr(info, data)


with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    with zipfile.ZipFile(ROOT / "mixed.zip", "w") as archive:
        write(archive, "padding.bin", bytes(256 * 1024))
        write(archive, "paper.xml", b"<article>hello</article>", zipfile.ZIP_DEFLATED)
        write(archive, "paper.pdf", b"%PDF-\x00\xff")
        write(archive, "empty.txt", b"")
        write(archive, "media/", b"")
        write(archive, "paper.xml", b"second version")
        write(archive, "../original.txt", b"path is data")
        write(archive, "expanded.txt", b"x" * 1024, zipfile.ZIP_DEFLATED)


class NonSeekable(io.BytesIO):
    def seek(self, *args):
        raise io.UnsupportedOperation("not seekable")


stream = NonSeekable()
with zipfile.ZipFile(stream, "w") as archive:
    write(archive, "stored.txt", b"stored descriptor")
    write(archive, "deflated.txt", b"deflated descriptor", zipfile.ZIP_DEFLATED)
(ROOT / "descriptors.zip").write_bytes(stream.getvalue())

with zipfile.ZipFile(ROOT / "zip64.zip", "w") as archive:
    info = zipfile.ZipInfo("paper.txt", (2026, 1, 1, 0, 0, 0))
    with archive.open(info, "w", force_zip64=True) as entry:
        entry.write(b"ZIP64")

# Add a real ZIP64 EOCD record and locator as well as the ZIP64 local header.
data = (ROOT / "zip64.zip").read_bytes()
eocd = len(data) - 22
_, _, _, count, _, size, offset, _ = struct.unpack_from("<4s4H2IH", data, eocd)
record = struct.pack("<4sQ2H2I4Q", b"PK\x06\x06", 44, 45, 45, 0, 0, count, count, size, offset)
locator = struct.pack("<4sIQI", b"PK\x06\x07", 0, eocd, 1)
end = struct.pack("<4s4H2IH", b"PK\x05\x06", 0, 0, 0xffff, 0xffff, 0xffffffff, 0xffffffff, 0)
(ROOT / "zip64.zip").write_bytes(data[:eocd] + record + locator + end)

with zipfile.ZipFile(ROOT / "empty.zip", "w"):
    pass

with zipfile.ZipFile(ROOT / "unsupported.zip", "w") as archive:
    write(archive, "bzip2.txt", b"bzip2 ZIP member", zipfile.ZIP_BZIP2)

# A malformed central-directory ZIP64 field with sizes but no required offset.
buffer = io.BytesIO()
with zipfile.ZipFile(buffer, "w") as archive:
    info = zipfile.ZipInfo("bad.txt", (2026, 1, 1, 0, 0, 0))
    info.extra = struct.pack("<HHQQ", 1, 16, 5, 5)
    archive.writestr(info, b"bytes")
data = bytearray(buffer.getvalue())
central = data.index(b"PK\x01\x02")
struct.pack_into("<I", data, central + 42, 0xffffffff)
(ROOT / "bad-offset.zip").write_bytes(data)
