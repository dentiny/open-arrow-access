# Library of Congress

Enable `service-library-of-congress`. Public access needs no key.
`search(parameters,page,count)` returns one full parsed JSON search page; use its
pagination metadata and `next_page(url)` for continuation. The caller chooses
query windows and observes LoC's published rate/result limits. Response format,
projection and pagination controls cannot be overridden accidentally by filters.

`item(id_or_url)` retrieves complete item metadata, including historical HTTP
item.id URLs upgraded to the same HTTPS API host. `resource(id_or_url)` resolves
the selected resource's advertised PDF/text/OCR links and expanded file groups.
All original metadata, rights and restriction fields remain unchanged. Logical
names identify resource-level fields or file/representation indices; no license
filter, format preference or URL deduplication is applied. Protocol-relative
content URLs are resolved to HTTPS without altering parsed metadata. Non-expanded
positive file counts fail rather than pretending there are no files.

`ocr(segment,format)` reads tile.loc.gov text-services with full_text=1 and keeps
the parsed JSON, including OCR text and any additional upstream fields. Files
are lazy sequential byte readers. Both API and text origins can be configured
independently for mirrors/tests. Pacing, retries, further resource discovery and
Lance conversion remain caller-owned.

Sources: [API endpoints](https://www.loc.gov/apis/json-and-yaml/requests/endpoints/),
[resource responses](https://www.loc.gov/apis/json-and-yaml/responses/item-and-resource/),
[text services](https://www.loc.gov/apis/micro-services/text-services/).
