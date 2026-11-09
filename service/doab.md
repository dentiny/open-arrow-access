# Doab

Enable `service-doab`. `DoabService::new()?` selects `https://directory.doabooks.org/oai/request`
and the upstream `xoai` metadata format. Use `records(from,until,set)` for
one initial ListRecords page, then `resume(token)` until no token remains.
Every page keeps exact XML and all returned fields, including deleted headers.
`with_connection` supports mirrors, custom transports and deterministic tests.
The protocol, parser, error handling and tests are shared through `oai`.

Callers own pacing and Retry-After handling. No credentials are loaded or required by this public harvest endpoint.
No records are filtered, deduplicated or normalized, and no Lance dataset is written.

Source: [provider metadata documentation](https://www.doabooks.org/en/librarians/doab-metadata).
