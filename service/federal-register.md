# Federal Register

Enable `service-federal-register`. `FederalRegisterService::new()` needs no key.
`search(parameters)` returns one parsed document-search page. Follow the upstream
`next_page_url` with `next_page(url)` instead of guessing page numbers: cursor
parameters and exact query encoding remain intact. Callers select date windows,
pace requests and record observed source metadata; mutable search results are
not presented as an immutable complete snapshot.

`document(document_number)` resolves a full document response and exposes the
advertised PDF, full-text XML, raw text, body HTML, web HTML and public-inspection
PDF under their original URL field names. Missing/null representations do not
drop metadata-only documents. Every upstream field stays in parsed JSON metadata.
Content downloads use lazy sequential readers; no content is fetched by resolution.
The shared optional-link utility handles mapping and validation, and existing
HTTP/reader contracts handle bytes. Source conversion and ingestion stay outside
this service.

Source: [Federal Register API documentation](https://www.federalregister.gov/developers/documentation/api/v1).
