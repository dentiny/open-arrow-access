# Common Crawl fixtures

Deterministic manifest examples for crawl `CC-MAIN-2022-40`, segment
`1664030338280.51`; these are synthetic membership samples, not crawl downloads.

| Input | Case |
| --- | --- |
| `crawl.paths`, `other_crawl.paths` | Crawl-wide membership and release filtering. |
| `good.paths` | Original order, CRLF, blank line, another segment, no final newline. |
| `one.paths` | One matching entry, also served by the injected HTTP transport. |
| `error_sequence.paths` | First/bad/last selections for downstream source errors. |
| `duplicate.paths`, `bad_*.paths` | Duplicate names, traversal, whitespace, absolute/external paths, query strings, percent encoding, suffixes, oversized lines, invalid UTF-8. |
| `empty.paths`, `blank.paths`, `other_crawl.paths`, `no_match.paths` | No matching selected files; the last uses a neighboring segment. |
| `other_duplicate.paths` | Duplicates outside the selected segment do not invalidate that segment. |
| `raw_manifest.paths.gz` | Gzip supplied directly to the text parser; must fail without decompression. |

## Use these inputs

Facade feature: `service-warc`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-service --no-default-features --features warc
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
