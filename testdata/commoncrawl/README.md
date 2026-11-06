# Common Crawl manifest fixtures

These small, checked-in manifests exercise segment discovery without a network
request. They use the example crawl `CC-MAIN-2022-40` and segment
`1664030338280.51`; they are deterministic test samples, not downloaded crawl data.

- `good.paths`: original order, CRLF, a blank line, another segment, no final newline.
- `one.paths`: one matching entry, also used by the injected HTTP transport.
- `error_sequence.paths`: first/bad/last records for injected downstream failures.
- `duplicate.paths`, `bad_*.paths`: duplicate entries, traversal, whitespace,
  absolute paths, external origins, query strings, percent encoding, suffix,
  oversized lines, and invalid UTF-8.
- `empty.paths`, `blank.paths`, `no_match.paths`: selections without matching files.
- `raw_manifest.paths.gz`: a valid gzip file deliberately supplied to the manifest
  text parser without a decompression layer; it must be rejected.

Tests read the exact bytes from disk, then use mocks only for chunk boundaries,
HTTP behavior, and injected failures. Preserve line endings and binary contents.
