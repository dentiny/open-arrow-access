# Changelog

## 0.1.0

Initial release of open-arrow-access, a composable Rust library for source access
and Apache Arrow record streams.

- Shared asynchronous accessor, reader, factory, layer, and scan-planning contracts.
- OpenDAL storage integration with sequential reads, exact ranges, and discovery.
- Opt-in Parquet, CSV/TSV, JSON/JSONL, XML, ATIF, and WARC readers.
- Streaming gzip, bzip2, LZ4, and Zstandard decompression, plus ZIP/TAR member access.
- Ordered file sets and directory datasets, including opt-in schema union by name.
- Source services for research repositories, books, web archives, and government data.
- Schema remapping, generated columns, seeded sampling, bounded parallel reads,
  and opt-in unordered emission for compatible sources.
- Usage guides, architecture diagrams, extension examples, and documented fixtures.

The default Cargo feature set is empty. Enable formats, codecs, datasets, and
services explicitly. This is an initial 0.1 release; public APIs may evolve in
subsequent minor releases.
