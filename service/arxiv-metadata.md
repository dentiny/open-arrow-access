# ArxivMetadata

Enable `service-arxiv-metadata`. `ArxivMetadataService::new()?` selects `https://oaipmh.arxiv.org/oai`
and the upstream `arXivRaw` metadata format. Use `records(from,until,set)` for
one initial ListRecords page, then `resume(token)` until no token remains.
Every page keeps exact XML and all returned fields, including deleted headers.
`with_connection` supports mirrors, custom transports and deterministic tests.
The protocol, parser, error handling and tests are shared through `oai`.

Callers own pacing and Retry-After handling. arXiv requests must be spaced by at least three seconds.
No records are filtered, deduplicated or normalized, and no Lance dataset is written.

Source: [provider metadata documentation](https://info.arxiv.org/help/oa/index.html).
