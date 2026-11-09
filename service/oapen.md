# Oapen

Enable `service-oapen`. `OapenService::new()?` selects `https://library.oapen.org/oai/request`
and the upstream `xoai` metadata format. Use `records(from,until,set)` for
one initial ListRecords page, then `resume(token)` until no token remains.
Every page keeps exact XML and all returned fields, including deleted headers.
`with_connection` supports mirrors, custom transports and deterministic tests.
The protocol, parser, error handling and tests are shared through `oai`.

Callers own pacing and Retry-After handling. No credentials are loaded or required by this public harvest endpoint.
No records are filtered, deduplicated or normalized, and no Lance dataset is written.

`open_book(url,options)` streams an OAPEN-hosted PDF/content URL from the harvested
metadata. It preserves the selected representation and rejects publisher-hosted
URLs, which require a separate source-specific fetch. No crawl is performed.

Source: [provider metadata documentation](https://oapen.org/article/metadata).
