# arXiv original documents and ancillary files

These small, project-authored fixtures are covered by this repository's MIT
license; they contain no downloaded arXiv articles. Tests load the committed
bytes without generating archives or modifying corrupt fields at runtime.

- `paper.pdf`, `main.tex`, and `paper.html` exercise original-byte reads. The PDF
  is a minimal one-page document. Single-file gzip variants include an empty
  TeX file.
- `source.tar` and `source.tar.gz` contain a directory, a manuscript, a PNG,
  four `anc/` files, and a duplicate `main.tex`. Regular members retain physical
  indices 1 through 7; `members/` and `duplicate.tex` hold expected payloads.
- `anc/` contains CSV, XML, a ZIP, and opaque synthetic MP4-like bytes. The latter
  checks byte preservation, not video decoding. The nested ZIP stays opaque.
- `binary-name.tar` contains a directory followed by a regular file named
  `\xff.tex` with payload `abc`, testing binary names and entry size limits.
- `bad-crc.tar.gz`, `truncated.tar.gz`, and `trailing.tar.gz` are serialized
  malformed variants testing checksum, incomplete trailer, and trailing bytes.
- `pdf/`, `src/`, and `html/` mirror article routes for local storage and HTTP
  transport tests of article `2410.00001v1`.

Run:

```sh
cargo test -p open-arrow-access-service --features arxiv --test arxiv_articles
cargo test --features service-arxiv --test arxiv_documents
```
