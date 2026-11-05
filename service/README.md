# Dataset services

The default-off `commoncrawl` feature implements a Common Crawl segment provider.
It resolves the crawl's official `warc.paths.gz` inventory, selects one segment's
WARC files, and delegates sequential reading to the dataset FileSet accessor.
See [Common Crawl segments](../docs/commoncrawl.md) for composition and limits.

Providers own discovery and dataset conventions, while existing byte, format and
record accessors own storage and decoding. The source-attribute proposal remains
a design for future partition planning; this provider does not enable parallel
reading, unordered emission, conversion jobs or a sink.
