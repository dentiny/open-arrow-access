# Test fixtures

These local inputs exercise readers, formats, byte layers, and services without
network access. Each directory documents its cases and a command for running
the owning component tests. Most samples are project-authored; the
[CourtListener excerpt](courtlistener/README.md) records its external source.

## Find an input

| Area | Fixtures |
| --- | --- |
| Byte contracts | [Bytes](bytes/README.md) |
| Formats | [CSV](csv/README.md), [JSON](json/README.md), [Parquet](parquet/README.md), [XML](xml/README.md), [ATIF](atif/README.md), [WARC](warc/README.md) |
| Compression / archives | [Gzip](gzip/README.md), [bzip2](bz2/README.md), [LZ4](lz4/README.md), [Zstandard](zstd/README.md), [ZIP](zip/README.md) |
| Providers | [Hugging Face](huggingface/README.md), [PMC](pmc/README.md), [Gutenberg](gutenberg/README.md), [Common Crawl](commoncrawl/README.md), [CourtListener](courtlistener/README.md) |
| Provider XML | [XML envelopes](xml-metadata/README.md) |

`openstax/book.zip` is a project-authored minimal GitHub source ZIP containing
one CNXML module; it verifies provider-to-ZIP composition without upstream book
content. Other provider directories contain their own API/manifest inputs.

## Use or add fixtures

Load bytes with `fixture!(directory, name)` or filesystem paths with
`fixture_path!(directory, name)` from [test support](../test-support/README.md).
Tests may copy files to isolated temporary roots. Keep expected values, schemas,
projections, requests, and assertions in test code.

Commit serialized inputs for both valid and invalid cases, and document their
purpose and provenance. Preserve duplicate keys, invalid UTF-8, line endings,
missing final newlines, and corrupt binary fields. Tests should not generate
inputs with encoders, inline documents, replacements, or binary corruption.

Test doubles may fragment bytes, inject I/O failures, model invalid decoder
output, or truncate an already-open temporary copy. Fixture HTTP transports may
select, paginate, and serialize committed entries. These controls test reader
behavior without replacing the recorded source inputs.
